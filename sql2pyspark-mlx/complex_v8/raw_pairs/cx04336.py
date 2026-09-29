from pyspark.sql import functions as F

valid_policys = (
    df_policies
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
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("claim_status_count"),
        F.max("claim_date").alias("last_claim_date"),
        F.count(F.when(F.col("status") == "APPROVED", 1)).alias("claim_status_count_2"),
    )
)

summarized_rows = (
    valid_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_status_count_2"), F.lit(0)).alias("claim_status_count_2"),
    )
)

result = (
    summarized_rows
    .groupBy("last_claim_date", "holder_id")
    .pivot("product", ["TRAVEL", "AUTO", "HEALTH", "HOME"])
    .agg(F.avg("premium"))
    .orderBy("last_claim_date", "holder_id")
)
