from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count = Window.partitionBy("row_count")
w_all_by_row_count_ties = Window.orderBy(F.col("row_count").asc_nulls_last())
w_claim_date_year = Window.partitionBy("claim_date_year")

base = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

summary = (
    base
    .filter(F.col("adjuster_id").isNotNull())
    .groupBy(F.year("claim_date").alias("claim_date_year"))
    .agg(
        F.min("days_since_reported_at").alias("min_days_since_reported_at"),
        F.round(F.stddev("days_since_reported_at"), 2).alias("stddev_days_since_reported_at"),
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("status") == "REJECTED", 1)) / F.count("*"), 1).alias("pct_status_rejected"),
    )
)

derived = (
    summary
    .select(
        "claim_date_year",
        "row_count",
        F.greatest(F.col("row_count"), F.col("pct_status_rejected")).alias("greatest_row_count_pct_status_rejected"),
        F.coalesce(F.col("row_count"), F.lit(0)).alias("row_count_filled"),
        (F.col("row_count") - F.col("claim_date_year")).alias("row_count_minus_claim_date_year"),
    )
)

windowed = (
    derived
    .select(
        "*",
        F.sum("claim_date_year").over(w_row_count).alias("row_count_sum_claim_date_year"),
        F.rank().over(w_all_by_row_count_ties).alias("row_count_rank"),
        F.min("claim_date_year").over(w_row_count).alias("row_count_min_claim_date_year"),
    )
)

annotated = (
    windowed
    .filter(F.col("greatest_row_count_pct_status_rejected") < 75.0)
    .select(
        "claim_date_year",
        "row_count",
        "greatest_row_count_pct_status_rejected",
        "row_count_filled",
        "row_count_minus_claim_date_year",
        "row_count_sum_claim_date_year",
        "row_count_rank",
        "row_count_min_claim_date_year",
        F.when(F.col("row_count_filled") >= 9, "A").when(F.col("row_count_filled") >= 4, "B").otherwise("C").alias("row_count_filled_tier"),
        (F.floor(F.col("row_count_rank") / 2) * 2).alias("row_count_rank_bucket"),
        F.least(F.col("row_count_rank"), F.col("row_count_filled")).alias("least_row_count_rank_row_count_filled"),
    )
)

with_windows = (
    annotated
    .select(
        "claim_date_year",
        "row_count",
        "greatest_row_count_pct_status_rejected",
        "row_count_filled",
        "row_count_rank_bucket",
        "least_row_count_rank_row_count_filled",
        F.min("row_count_rank").over(w_claim_date_year).alias("claim_date_year_min_row_count_rank"),
        F.size(F.collect_set("row_count_filled_tier").over(w_claim_date_year)).alias("distinct_row_count_filled_tier_in_claim_date_yea"),
    )
)

enriched = (
    with_windows
    .filter(F.col("row_count_rank_bucket") > 3)
    .withColumn("row_count_minus_row_count_rank_bucket", F.col("row_count") - F.col("row_count_rank_bucket"))
)

result = (
    enriched
    .select(
        "claim_date_year",
        "row_count",
        "row_count_filled",
        "row_count_rank_bucket",
        "least_row_count_rank_row_count_filled",
        "distinct_row_count_filled_tier_in_claim_date_yea",
        "row_count_minus_row_count_rank_bucket",
        F.abs(F.col("row_count_filled") - 8).alias("row_count_filled_distance"),
        F.coalesce(F.col("row_count_rank_bucket"), F.lit(0)).alias("row_count_rank_bucket_filled"),
    )
    .orderBy("claim_date_year", F.col("row_count").desc())
    .limit(20)
)
