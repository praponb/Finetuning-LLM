from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_id", "term").orderBy("score_filled", "id")

cleaned = (
    df_enrollments
    .filter(F.col("grade_status").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref_2"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
        F.min("score").alias("min_enrollment_score"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        "e.min_enrollment_score",
    )
)

latest_per_key = (
    summarized_rows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .filter(F.col("score_filled").between(20.0, 65.0))
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_ref",
        "grade_status_ref_2",
        "enrollment_count",
        "enrollment_count_2",
        "min_enrollment_score",
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
        F.when(F.col("grade_status").isin("FAIL", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
    )
    .orderBy(F.col("course_id").desc_nulls_first())
)
