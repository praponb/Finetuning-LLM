from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_credits = Window.orderBy("credits", "id")
w_all_by_credits_running = Window.orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_dept_len_dept_by = Window.partitionBy("last_known_dept_len", "dept").orderBy("last_known_dept_len", "id", "enrollment_id")

valid_courses = (
    df_courses
    .filter(F.upper(F.trim(F.col("course_code"))).like("C%"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
        F.when(F.col("level").isin("advanced", "intro"), "group_a").when(F.col("level") == "core", "group_b").otherwise("other").alias("level_group"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
    )
)

with_windows = (
    valid_courses
    .select(
        "id",
        "dept",
        "dept_len",
        "level_group",
        F.row_number().over(w_all_by_credits).alias("credits_seq"),
        F.last("dept_len", ignorenulls=True).over(w_all_by_credits_running).alias("last_known_dept_len"),
        F.lead("dept_len", 1, 0).over(w_all_by_credits).alias("next_dept_len"),
    )
)

exploded_detail_enrollments = (
    with_windows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id") <= 9), "inner")
    .select(
        "s.id",
        "s.dept",
        "s.level_group",
        "s.last_known_dept_len",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
    )
)

with_activity = (
    exploded_detail_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.term") != "2025T2") & (F.col("e.score") > 5.7), "left_anti")
)

metrics = (
    with_activity
    .withColumn("minutes_since_prev_enrollment_enrolled_at", F.round((F.unix_timestamp("enrollment_enrolled_at") - F.unix_timestamp(F.lag("enrollment_enrolled_at").over(w_last_known_dept_len_dept_by))) / 60, 1))
    .withColumn("last_known_dept_len_seq", F.row_number().over(w_last_known_dept_len_dept_by))
)

result = (
    metrics
    .select(
        "id",
        "dept",
        "level_group",
        "last_known_dept_len",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_enrolled_at",
        "enrollment_term",
        "minutes_since_prev_enrollment_enrolled_at",
    )
    .orderBy(F.col("enrollment_enrolled_at").desc())
)
