from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_segment", "status").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

recent_claims = df_claims.filter(F.col("claim_type").isNotNull())

enriched = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("po.segment").alias("holder_segment"),
    )
)

latest = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

exploded_detail_claims = (
    latest.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.policy_product",
        "s.policy_holder_id",
        "s.holder_segment",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    exploded_detail_claims
    .select(
        "id",
        "claim_id",
        "claim_approved_amount",
        "claimed_amount",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_approved_amount", "claimed_amount"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
