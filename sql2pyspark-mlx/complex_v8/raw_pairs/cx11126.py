from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(7, 12)) & (F.col("approved_amount") > 799.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
    )
)

enriched_policy = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.region").alias("holder_region"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

with_stats = (
    enriched_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    with_stats
    .select("id", "approved_amount", "approved_amount_adj")
    .unpivot(
        ids=["id"],
        values=["approved_amount", "approved_amount_adj"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
