from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-05-30").cast("date")) & (F.col("end_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_parent_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

derived = (
    with_parent_holder
    .select(
        "id",
        "holder_id",
        "holder_segment",
        "holder_name",
        F.when(F.col("holder_risk_score") >= 0.6, "A").when(F.col("holder_risk_score") >= 0.2, "B").otherwise("C").alias("holder_risk_score_tier"),
        F.when(F.col("holder_segment").isin("SME", "VIP"), "group_a").when(F.col("holder_segment") == "RETAIL", "group_b").otherwise("other").alias("holder_segment_group"),
    )
)

enriched = (
    derived
    .select(
        "*",
        F.lower(F.trim(F.col("holder_name"))).alias("holder_name_clean"),
        F.substring(F.trim(F.col("holder_name")), 1, 2).alias("holder_name_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.total_claim_approved_amount",
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    profiled
    .groupBy("holder_id")
    .pivot("holder_segment_group", ["group_b", "other", "group_a"])
    .agg(F.min("total_claim_approved_amount"))
)
