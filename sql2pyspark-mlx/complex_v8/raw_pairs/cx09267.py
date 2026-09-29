from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-12-05").cast("date")) & (F.col("product") == "HEALTH"))
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
    )
)

summarized_rows = (
    valid_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.total_claim_claimed_amount", "c.min_claim_approved_amount")
)

result = (
    summarized_rows
    .groupingSets(
        [["holder_id", "status"], ["holder_id"], []],
        "holder_id", "status",
    )
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.round(100 * F.count(F.when(F.col("product") == "HEALTH", 1)) / F.count("*"), 1).alias("pct_product_health"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "last_end_date",
        "total_premium",
        "pct_product_health",
    )
    .orderBy(F.col("holder_id").asc_nulls_last(), F.col("status").asc_nulls_last())
)
