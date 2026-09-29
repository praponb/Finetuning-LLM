from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("enrollment_student_id", "enrollment_grade_status").orderBy(F.col("enrollment_enrolled_at").desc(), F.col("id").desc(), F.col("enrollment_id").desc())
w_enrollment_enrolled_at_month = Window.partitionBy(F.date_trunc("month", "enrollment_enrolled_at")).orderBy("enrollment_enrolled_at")
w_level_by_enrollment_enrolled = Window.partitionBy("level").orderBy("enrollment_enrolled_at", "id", "enrollment_id")
w_level_enrollment_enrolled_at = Window.partitionBy("level", "enrollment_enrolled_at_rank").orderBy("enrollment_enrolled_at", "id", "enrollment_id")
w_enrollment_student_id_level = Window.partitionBy("enrollment_student_id", "level")
w_enrollment_student_id_by_enr = Window.partitionBy("enrollment_student_id").orderBy(F.col("enrollment_enrolled_at").desc(), "id", "enrollment_id")

recent = (
    df_courses
    .filter((F.col("dept") != "Biz") & (F.col("level").isin("intro", "advanced")))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.upper(F.trim(F.col("dept"))).alias("dept_clean"),
    )
)

with_children = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.course_code",
        "s.level",
        "s.dept_clean",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

unique_rows = (
    with_children
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

sequenced = (
    unique_rows
    .select(
        "*",
        F.rank().over(w_enrollment_enrolled_at_month).alias("enrollment_enrolled_at_rank"),
        F.lag("enrollment_enrolled_at").over(w_level_by_enrollment_enrolled).alias("prev_enrollment_enrolled_at"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.round((F.unix_timestamp("enrollment_enrolled_at") - F.unix_timestamp(F.lag("enrollment_enrolled_at").over(w_level_enrollment_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrollment_enrolled_at"),
        F.round(F.col("enrollment_enrolled_at_rank") - F.avg("enrollment_enrolled_at_rank").over(w_enrollment_student_id_level), 2).alias("enrollment_enrolled_at_rank_vs_enrollment_studen"),
        F.count(F.lit(1)).over(w_enrollment_student_id_level).alias("rows_in_enrollment_student_id_level"),
        F.round((F.unix_timestamp("enrollment_enrolled_at") - F.unix_timestamp(F.lag("enrollment_enrolled_at").over(w_enrollment_student_id_by_enr))) / 60, 1).alias("minutes_since_prev_enrollment_enrolled_at_2"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrollment_enrolled_at")).alias("days_since_enrollment_enrolled_at"),
        F.concat(F.col("level"), F.lit("#"), F.col("id").cast("string")).alias("level_ref"),
        F.date_trunc("month", F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_month"),
    )
)

annotated = (
    derived
    .filter(F.col("days_since_enrollment_enrolled_at") < 560)
    .select(
        "id",
        "course_code",
        "level",
        "dept_clean",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_student_id",
        "enrollment_enrolled_at",
        "enrollment_enrolled_at_rank",
        "prev_enrollment_enrolled_at",
        "minutes_since_prev_enrollment_enrolled_at",
        "days_since_enrollment_enrolled_at",
        "level_ref",
        "enrollment_enrolled_at_month",
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
)

result = (
    annotated
    .select(
        "id",
        "course_code",
        "level",
        "dept_clean",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_enrolled_at",
        "enrollment_enrolled_at_rank",
        "prev_enrollment_enrolled_at",
        "days_since_enrollment_enrolled_at",
        "level_ref",
        "enrollment_enrolled_at_month",
        "course_code_len",
        F.coalesce(F.col("course_code_len"), F.lit(0)).alias("course_code_len_filled"),
    )
    .orderBy(
        F.col("days_since_enrollment_enrolled_at").desc(),
        F.col("enrollment_grade_status").desc(),
        "id",
        "enrollment_id",
    )
    .limit(5)
)
