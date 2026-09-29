from pyspark.sql import functions as F

valid = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.add_months(F.col("end_date"), 12).alias("end_date_plus_12m"),
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status").isin("APPROVED", "OPEN"), 1)).alias("claim_status_count"),
    )
)

with_counts = (
    valid.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

result = (
    with_counts
    .groupBy("status", "claim_status_count")
    .pivot("product", ["HEALTH", "AUTO", "HOME"])
    .agg(F.max("premium"))
    .orderBy("status", "claim_status_count")
)
