from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    exploded
    .groupBy("currency")
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("element")).otherwise(0)).alias("thb_element"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.min("element").alias("min_element"),
    )
    .filter(F.col("thb_element") < 1000000)
    .orderBy(F.col("min_element").desc())
)
