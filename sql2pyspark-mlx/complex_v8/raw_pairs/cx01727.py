from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("term").isin("2025T2", "2024T1", "2025T1")) & (F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at_2"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

term_stats = (
    prepared_enrollments
    .groupBy("term")
    .agg(F.avg("days_since_enrolled_at").alias("avg_days_since_enrolled_at"))
)

outliers = (
    prepared_enrollments.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.days_since_enrolled_at") > F.col("g.avg_days_since_enrolled_at"))
    .select(
        "s.*",
        "g.avg_days_since_enrolled_at",
        F.round(F.col("s.days_since_enrolled_at") - F.col("g.avg_days_since_enrolled_at"), 2).alias("days_since_enrolled_at_minus_avg"),
    )
)

totals = (
    outliers
    .groupBy("grade_status")
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("grade_status") == "WITHDRAWN", F.col("score_bucket")).otherwise(0)).alias("score_bucket_grade_status_withdrawn"),
        F.max("score_bucket").alias("max_score_bucket"),
    )
)

per_label = (
    totals
    .select(
        "grade_status",
        "cnt",
        "score_bucket_grade_status_withdrawn",
        "max_score_bucket",
        F.explode_outer("term_list").alias("term_list_item"),
    )
)

result = (
    per_label
    .filter(F.col("grade_status").isNotNull())
    .select(
        "grade_status",
        "cnt",
        "score_bucket_grade_status_withdrawn",
        "max_score_bucket",
        "term_list_item",
    )
    .orderBy(F.col("grade_status").desc())
)
