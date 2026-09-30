from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_term_in_2024t2_2024t1_2025 = Window.partitionBy("n_term_in_2024t2_2024t1_2025t2", "distinct_ids")
w_all_by_distinct_ids_moving_2 = Window.orderBy("distinct_ids", "term").rowsBetween(-2, Window.currentRow)
w_all_by_distinct_ids = Window.orderBy("distinct_ids", "term")

recent_enrollments = (
    df_enrollments
    .filter(~F.col("grade_status").isin("WITHDRAWN", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at_2"),
    )
)

grouped = (
    recent_enrollments
    .groupBy("term")
    .agg(
        F.count(F.when(F.col("term").isin("2024T2", "2024T1", "2025T2"), 1)).alias("n_term_in_2024t2_2024t1_2025t2"),
        F.sum(F.when(F.col("term") == "2024T2", F.col("days_since_enrolled_at_2"))).alias("days_since_enrolled_at_2_term_2024t2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(~F.col("term").isin("2025T1", "2025T2"), 1)) / F.count("*"), 1).alias("pct_term_not_in_2025t1_2025t2"),
        F.round(100 * F.count(F.when(F.col("term") == "2024T1", 1)) / F.count("*"), 1).alias("pct_term_2024t1"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 10.1)
    .drop("avg_score_check")
)

annotated = (
    grouped
    .select(
        "term",
        "n_term_in_2024t2_2024t1_2025t2",
        "days_since_enrolled_at_2_term_2024t2",
        "distinct_ids",
        "pct_term_not_in_2025t1_2025t2",
        "pct_term_2024t1",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        (F.col("pct_term_2024t1") + F.col("distinct_ids")).alias("pct_term_2024t1_plus_distinct_ids"),
        F.concat(F.col("term"), F.lit("#"), F.col("distinct_ids").cast("string")).alias("term_ref"),
    )
)

sequenced = (
    annotated
    .select(
        "*",
        F.min("distinct_ids").over(w_n_term_in_2024t2_2024t1_2025).alias("n_term_in_2024t2_2024t1_2025t2_distinct_ids_min_"),
        F.max("distinct_ids").over(w_all_by_distinct_ids_moving_2).alias("distinct_ids_max_3"),
        F.round(F.cume_dist().over(w_all_by_distinct_ids), 3).alias("distinct_ids_cume_dist"),
        F.max("term_len").over(w_all_by_distinct_ids_moving_2).alias("term_len_max_3"),
    )
)

with_activity = (
    sequenced
    .crossJoin(sequenced.agg(F.max("term_len").alias("overall_max_term_len")))
    .filter(F.col("term_len") < F.col("overall_max_term_len"))
    .drop("overall_max_term_len")
)

result = (
    with_activity
    .select(
        "term",
        "n_term_in_2024t2_2024t1_2025t2",
        "days_since_enrolled_at_2_term_2024t2",
        "distinct_ids",
        "pct_term_not_in_2025t1_2025t2",
        "term_len",
        "pct_term_2024t1_plus_distinct_ids",
        "term_ref",
        "n_term_in_2024t2_2024t1_2025t2_distinct_ids_min_",
        "distinct_ids_max_3",
        "distinct_ids_cume_dist",
        "term_len_max_3",
        F.length(F.trim(F.col("term_ref"))).alias("term_ref_len"),
    )
    .orderBy(F.col("distinct_ids_max_3").asc(), "term")
    .limit(5)
)
