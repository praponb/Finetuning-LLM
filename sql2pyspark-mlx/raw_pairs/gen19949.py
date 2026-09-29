from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-03-27").cast("date"))
    .select("*", F.round((F.col("quantity") * 100), 2).alias("quantity_adjusted"))
)

result = (
    cleaned
    .groupBy("store_id")
    .agg(
        F.min("quantity_adjusted").alias("min_quantity_adjusted"),
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("event_count"),
        F.percentile_approx("quantity_adjusted", 0.9).alias("quantity_adjusted_p90"),
    )
    .filter(F.col("min_quantity") > 0)
    .orderBy(F.col("event_count").desc())
)
