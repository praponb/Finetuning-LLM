from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id").orderBy(F.col("reported_at_date").desc(), F.col("id").desc())

base_claims = (
    df_claims
    .filter(F.col("claim_type") == "flood")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.quarter(F.col("reported_at")).alias("reported_at_quarter"),
    )
)

latest_per_key = (
    base_claims
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_children_claims = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.to_date(F.col("c.reported_at")) < F.lit("2025-03-02").cast("date")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status_clean",
        "s.reported_at_quarter",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    with_children_claims
    .groupBy("claim_id", "claim_reported_at")
    .pivot("claim_claim_type", ["delay", "medical", "collision", "fire"])
    .agg(F.sum("reported_at_quarter").alias("amount"), F.max("reported_at_quarter").alias("max_v"))
    .orderBy("claim_id", "claim_reported_at")
)
