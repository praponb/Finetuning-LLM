from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 74, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .rollup("plan", "tier_value_tier", "quarter_start")
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.sum("tier_value").alias("total_tier_value"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("tier_value"))).alias("eur_total_tier_value"),
        F.grouping("plan").alias("gid"),
    )
    .filter(F.col("basic_events") < 1000)
)
