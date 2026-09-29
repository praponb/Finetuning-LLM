from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("claim_type").isin("collision", "fire"))
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
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_reported_at",
        "c.last_claim_reported_at",
    )
)

result = (
    with_counts
    .groupingSets(
        [["status", "claim_type"], ["status"]],
        "status", "claim_type",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("claim_count").alias("total_claim_count"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "grouping_level",
        "total_claim_count",
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("claim_type").asc_nulls_last())
)
