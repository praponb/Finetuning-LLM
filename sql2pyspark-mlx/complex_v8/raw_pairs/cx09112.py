from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("status").isin("LAPSED", "CANCELLED", "ACTIVE")) & (F.col("premium") > 1132.1))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.concat_ws("|", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type") != "collision", 1)).alias("claim_claim_type_count"),
        F.count("*").alias("claim_count"),
        F.count(F.when(~F.col("status").isin("OPEN", "APPROVED"), 1)).alias("claim_status_count"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

result = (
    with_stats
    .select("id", "claim_status_count", "claim_count")
    .unpivot(
        ids=["id"],
        values=["claim_status_count", "claim_count"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
