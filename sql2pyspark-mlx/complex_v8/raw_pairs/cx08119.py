from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), F.col("id").desc())

prepared_claims = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

combined_details_policy = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

filtered = (
    combined_details_policy.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status").isin("ACTIVE", "CANCELLED", "EXPIRED")) & (F.year(F.col("p.end_date")) == 2026), "left_anti")
)

unique_rows = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"))
)

with_counts = (
    unique_rows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.avg_claim_claimed_amount")
)

result = (
    with_counts
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "policy_premium",
        "policy_status",
        "avg_claim_claimed_amount",
        F.round(F.col("avg_claim_claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("avg_claim_claimed_amount_per_approved_amount"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
    .orderBy(F.col("policy_id").desc())
)
