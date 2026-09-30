from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        F.when(F.col("tier_value") >= 85, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.max("tier_value").alias("max_tier_value"),
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value")).otherwise(0)).alias("thb_tier_value"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.avg(F.when(F.col("currency") == "THB", F.col("tier_value"))).alias("thb_avg_tier_value"),
    )
    .filter(F.col("num_plans") > 1)
    .orderBy("max_tier_value")
)
