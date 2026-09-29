from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

profiled = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_date", "c.last_claim_date")
)

result = (
    profiled
    .groupBy("holder_id")
    .pivot("product", ["TRAVEL", "HOME", "LIFE", "HEALTH"])
    .agg(F.avg("status_len").alias("avg_v"), F.max("status_len").alias("max_v"))
    .orderBy("holder_id")
)
