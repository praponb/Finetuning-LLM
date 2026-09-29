from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("course_id", "grade_status").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("student_id").orderBy("enrollment_enrolled_at", "id", "enrollment_id")

valid_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-02-06 06:00:00").cast("timestamp")) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

unique_rows = (
    valid_enrollments
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

exploded_detail = (
    unique_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

unique_rows_2 = (
    exploded_detail
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows_2
    .filter(F.col("enrollment_term") != "2025T1")
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_term",
        "enrollment_enrolled_at",
    )
    .orderBy(F.col("grade_status").desc())
)
