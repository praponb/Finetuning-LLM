from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-12-02").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("approved_amount").alias("min_claim_approved_amount"))
)

with_stats = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.min_claim_approved_amount")
)

labeled = with_stats.withColumn("end_date_month", F.date_trunc("month", F.col("end_date")))

joined_holder = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.product_ref",
        "s.days_start_date_to_end_date",
        "s.min_claim_approved_amount",
        "s.end_date_month",
        "p.holder_name",
    )
)

result = (
    joined_holder
    .groupBy("end_date")
    .pivot("product", ["AUTO", "HOME", "HEALTH", "LIFE"])
    .agg(F.min("min_claim_approved_amount"))
    .orderBy("end_date")
)
