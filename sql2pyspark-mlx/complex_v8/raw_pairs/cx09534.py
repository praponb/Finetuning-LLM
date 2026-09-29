from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_30d"),
    )
)

combined_details = (
    joined_ranges.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("po.segment").alias("holder_segment"),
    )
)

enriched = (
    combined_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "policy_id",
        "policy_start_date",
        "policy_end_date",
        "policy_holder_id",
        "holder_segment",
        "policy_premium",
    )
    .orderBy("policy_start_date", "id")
    .limit(20)
)
