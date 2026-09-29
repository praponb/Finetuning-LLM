from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("product") == "LIFE") & (F.col("premium") <= 3532.7))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
)

joined_holder = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

qualified = (
    joined_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.reported_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) & (F.col("c.approved_amount") > 1273.7), "left_anti")
    .filter(F.col("holder_id").isNotNull())
)

result = (
    qualified
    .groupBy("start_date", "holder_id")
    .pivot("product", ["AUTO", "LIFE", "HOME"])
    .agg(F.avg("premium"))
)
