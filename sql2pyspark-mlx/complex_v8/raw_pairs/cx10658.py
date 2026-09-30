from pyspark.sql import functions as F

filtered = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.sum("premium").alias("total_policy_premium"),
        F.max("start_date").alias("last_policy_start_date"),
        F.count("*").alias("policy_count"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.total_policy_premium",
        "p.last_policy_start_date",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

with_details_holder = (
    with_stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.premium_filled",
        "s.start_date_label",
        "s.total_policy_premium",
        "s.last_policy_start_date",
        "s.policy_count",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_children = (
    with_details_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-30").cast("date"))), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_filled",
        "s.start_date_label",
        "s.total_policy_premium",
        "s.policy_count",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_date",
    )
)

result = (
    with_children
    .cube("claim_id", "holder_id")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("total_policy_premium", 0.5).alias("median_total_policy_premium"),
        F.min("claim_date").alias("first_claim_date"),
    )
    .orderBy(F.col("claim_id").asc_nulls_last(), F.col("holder_id").asc_nulls_last())
)
