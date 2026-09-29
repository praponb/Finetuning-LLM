from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("dept_group", "dept").orderBy(F.col("credits").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("dept_group", "dept").orderBy("credits", "id")
w_enrollment_id_2_by_enrollmen = Window.partitionBy("enrollment_id_2").orderBy(F.col("enrollment_enrolled_at").desc(), "id", "enrollment_id", "enrollment_id_2")
w_all_by_enrollment_score_2 = Window.orderBy(F.col("enrollment_score_2").asc_nulls_last(), "id", "enrollment_id", "enrollment_id_2")
w_enrollment_grade_status = Window.partitionBy("enrollment_grade_status")

scoped = (
    df_courses
    .filter(F.upper(F.trim(F.col("course_code"))).like("C%"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("dept").isin("CS", "Bio"), "group_a").when(F.col("dept") == "Art", "group_b").otherwise("other").alias("dept_group"),
        F.concat(F.col("course_code"), F.lit("#"), F.col("credits").cast("string")).alias("course_code_ref"),
        F.lower(F.trim(F.col("level"))).alias("level_clean"),
    )
)

unique_rows = (
    scoped
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    unique_rows
    .withColumn("rn", F.row_number().over(w_latest_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_children = (
    deduped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.dept",
        "s.credits",
        "s.dept_group",
        "s.course_code_ref",
        "s.level_clean",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

detail_rows = (
    with_children.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.enrolled_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.dept_group",
        "s.course_code_ref",
        "s.level_clean",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrollment_grade_status",
        "s.enrollment_enrolled_at",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.student_id").alias("enrollment_student_id_2"),
        F.col("e.score").alias("enrollment_score_2"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
    )
)

sequenced = (
    detail_rows
    .select(
        "*",
        F.lag("enrollment_enrolled_at", 3).over(w_enrollment_id_2_by_enrollmen).alias("prev3_enrollment_enrolled_at"),
        F.row_number().over(w_all_by_enrollment_score_2).alias("enrollment_score_2_seq"),
        F.round(F.col("enrollment_score_2") / F.nullif(F.sum("enrollment_score_2").over(w_enrollment_grade_status), F.lit(0)), 4).alias("enrollment_score_2_share_of_enrollment_grade_sta"),
    )
)

result = (
    sequenced
    .cube("enrollment_id", "enrollment_id_2", "enrollment_student_id_2")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("enrollment_score_2_seq").alias("total_enrollment_score_2_seq"),
        F.round(F.stddev("enrollment_score_2_share_of_enrollment_grade_sta"), 2).alias("stddev_enrollment_score_2_share_of_enrollment_gr"),
        F.count(F.when(F.col("enrollment_grade_status") == "WITHDRAWN", 1)).alias("n_enrollment_grade_status_with"),
    )
)
