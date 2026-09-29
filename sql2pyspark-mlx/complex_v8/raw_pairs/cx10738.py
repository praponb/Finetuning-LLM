from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("approved_amount") > 492.6) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

rollup_base = (
    base
    .groupBy(F.year("reported_at").alias("reported_at_year"), "policy_id", "status")
    .agg(
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.round(100 * F.count(F.when(F.col("claim_type").isin("flood", "delay", "theft"), 1)) / F.count("*"), 1).alias("pct_claim_type_in_flood_delay_th"),
        F.avg("claimed_amount").alias("avg_claimed_amount_check"),
    )
    .filter(F.col("avg_claimed_amount_check") > 11235.6)
    .drop("avg_claimed_amount_check")
)

with_activity = (
    rollup_base.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") >= F.lit("2024-03-30").cast("date")), "left_semi")
)

combined_details = (
    with_activity.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.reported_at_year",
        "s.policy_id",
        "s.status",
        "s.max_claimed_amount",
        "s.pct_claim_type_in_flood_delay_th",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

result = (
    combined_details
    .select(
        "reported_at_year",
        "policy_id",
        "status",
        "pct_claim_type_in_flood_delay_th",
        "policy_holder_id",
        F.round(F.col("max_claimed_amount") * 1.1, 2).alias("max_claimed_amount_adj"),
    )
    .orderBy(
        F.col("max_claimed_amount_adj").asc(),
        "pct_claim_type_in_flood_delay_th",
    )
)
