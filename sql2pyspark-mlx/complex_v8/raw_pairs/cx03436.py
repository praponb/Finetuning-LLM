from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("holder_id") <= 14)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.count(F.when(~F.col("status").isin("REJECTED", "OPEN"), 1)).alias("claim_status_count"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.total_claim_approved_amount",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.last_claim_date",
    )
)

result = (
    with_stats
    .groupBy("start_date")
    .pivot("product", ["TRAVEL", "LIFE"])
    .agg(F.max("premium"))
    .orderBy("start_date")
)
