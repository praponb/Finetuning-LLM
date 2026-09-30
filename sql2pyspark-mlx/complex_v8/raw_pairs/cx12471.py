from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("enrollment_grade_status_count").desc(), F.col("id").desc())

valid_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("grade_status") != "PASS", 1)).alias("enrollment_grade_status_count"),
    )
)

with_counts = (
    valid_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

enriched = (
    with_counts
    .select(
        "*",
        F.abs(F.col("score_bucket") - 40).alias("score_bucket_distance"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

unique_rows = (
    enriched
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "score_bucket",
        "score_bucket_distance",
        F.when(F.col("term").isin("2024T2", "2025T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.when(F.col("score_bucket") >= 85, "HIGH").when(F.col("score_bucket") >= 35, "MEDIUM").otherwise("LOW").alias("score_bucket_tier"),
    )
    .orderBy(F.col("score_bucket_tier").desc())
)
