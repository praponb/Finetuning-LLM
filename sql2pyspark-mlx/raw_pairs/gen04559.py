from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("element"))
)

result = (
    exploded
    .withColumn("element_tier", F.when(F.col("element") >= 77, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .cube("element_tier", "week_start")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.grouping("element_tier").alias("grouping_id"),
    )
    .filter(F.col("avg_element") < 1000000)
)
