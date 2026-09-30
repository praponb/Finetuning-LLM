from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

result = (
    exploded
    .withColumn("val_tier", F.when(F.col("val") >= 63, "HIGH").otherwise("LOW"))
    .groupingSets([["val_tier", "currency"], ["val_tier"]], "val_tier", "currency")
    .agg(
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.sum(F.when(F.col("currency") == "THB", F.col("val")).otherwise(0)).alias("thb_val"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
    )
    .filter(F.col("thb_val") >= 0)
)
