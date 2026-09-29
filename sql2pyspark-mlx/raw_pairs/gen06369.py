from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 86, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("plan") == "basic", F.col("element")).otherwise(0)).alias("basic_element"),
        F.count("*").alias("num_rows"),
        F.min("element").alias("min_element"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
    )
)
