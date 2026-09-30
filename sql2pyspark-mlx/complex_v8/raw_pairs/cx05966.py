from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) | (F.col("policy_id") >= 20))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.count("*").alias("claim_count"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
    )
)

with_stats = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.min_claim_claimed_amount",
    )
)

status_stats = (
    with_stats
    .groupBy("status")
    .agg(F.max("claimed_amount").alias("max_claimed_amount"))
)

above_average = (
    with_stats.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.claimed_amount") == F.col("g.max_claimed_amount"))
    .select(
        "s.*",
        "g.max_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.max_claimed_amount"), 2).alias("claimed_amount_minus_max"),
    )
)

result = (
    above_average
    .select("id", "approved_amount", "max_claimed_amount")
    .unpivot(
        ids=["id"],
        values=["approved_amount", "max_claimed_amount"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
