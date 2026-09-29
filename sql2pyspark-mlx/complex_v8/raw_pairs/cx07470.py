from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_status_is_not_nul = Window.orderBy(F.col("pct_status_is_not_null").asc_nulls_last(), "approved_amount_tier_2", "policy_id")

base = (
    df_claims
    .filter(F.col("claim_type") == "fire")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 20880.4, "HIGH").when(F.col("approved_amount") >= 2272.7, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

rollup_base = (
    base
    .groupBy(
        F.when(F.col("approved_amount") >= 19337.6, "A").when(F.col("approved_amount") >= 0.0, "B").otherwise("C").alias("approved_amount_tier_2"),
        "policy_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.max("reported_at").alias("last_reported_at"),
        F.count("*").alias("cnt"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
    )
)

ranked = (
    rollup_base
    .select(
        "approved_amount_tier_2",
        "policy_id",
        "pct_status_is_not_null",
        "claim_type_list",
        "last_reported_at",
        "claim_date_span_days",
        F.round((F.unix_timestamp("last_reported_at") - F.unix_timestamp(F.lag("last_reported_at").over(w_all_by_pct_status_is_not_nul))) / 60, 1).alias("minutes_since_prev_last_reported_at"),
        F.row_number().over(w_all_by_pct_status_is_not_nul).alias("seq_no"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("claim_type") == "delay", 1)).alias("claim_claim_type_count"),
    )
)

with_counts = (
    ranked.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    with_counts
    .groupBy("claim_claim_type_count", "last_reported_at")
    .pivot("approved_amount_tier_2", ["C", "B", "A"])
    .agg(F.sum("claim_date_span_days").alias("total"), F.count("claim_date_span_days").alias("n"))
    .orderBy("claim_claim_type_count", "last_reported_at")
)
