from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("course_id").orderBy(F.col("enrolled_at_date").desc(), F.col("id").desc())

recent = (
    df_enrollments
    .filter((F.col("course_id") <= 6) & (F.col("score") >= 10.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    recent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

latest_per_key = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupBy("enrollment_count", "enrolled_at")
    .pivot("grade_status", ["WITHDRAWN", "PASS", "INCOMPLETE"])
    .agg(F.sum("score"))
    .orderBy("enrollment_count", "enrolled_at")
)
