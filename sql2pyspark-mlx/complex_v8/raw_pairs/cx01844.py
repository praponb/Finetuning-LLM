from pyspark.sql import functions as F

cleaned_policys = df_policies.filter(F.col("product") == "HOME")

joined_holder = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
    )
)

joined_holder_2 = (
    joined_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.holder_segment",
        F.col("p.holder_name").alias("holder_holder_name"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment_2"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_stats = (
    joined_holder_2.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.min_claim_approved_amount", "c.last_claim_reported_at")
)

result = (
    with_stats
    .groupingSets(
        [["holder_segment", "holder_id"], ["holder_segment"], []],
        "holder_segment", "holder_id",
    )
    .agg(
        F.sum(F.when(~F.col("holder_segment").isin("VIP", "SME"), 1).otherwise(0)).alias("n_holder_segment_not_in_vip_sm"),
        F.max("holder_risk_score").alias("max_holder_risk_score"),
    )
    .orderBy(
        F.col("holder_segment").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
