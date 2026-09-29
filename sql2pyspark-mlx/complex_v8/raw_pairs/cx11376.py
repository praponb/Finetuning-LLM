from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrollment_term_count = Window.orderBy("enrollment_term_count")
w_enrolled_at_to_date_term = Window.partitionBy(F.to_date("enrolled_at"), "term")
w_latest = Window.partitionBy("term").orderBy(F.col("enrollment_term_count").desc(), F.col("id").desc())

base_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") != "WITHDRAWN") & (F.col("score") <= 57.6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count(F.when(F.col("term") == "2024T1", 1)).alias("enrollment_term_count"),
    )
)

with_stats = (
    base_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

ranked = (
    with_stats
    .select(
        "*",
        F.rank().over(w_all_by_enrollment_term_count).alias("enrollment_term_count_rank"),
        F.round(F.col("grade_status_len") - F.avg("grade_status_len").over(w_enrolled_at_to_date_term), 2).alias("grade_status_len_vs_enrolled_at_to_date_term_avg"),
    )
)

deduped = (
    ranked
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .filter(F.col("course_id") <= 2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status_len",
        "min_enrollment_score",
        "last_enrollment_enrolled_at",
        "enrollment_term_count",
        "grade_status_len_vs_enrolled_at_to_date_term_avg",
    )
    .orderBy(
        F.col("id").desc(),
        F.col("grade_status_len_vs_enrolled_at_to_date_term_avg").desc(),
    )
)
