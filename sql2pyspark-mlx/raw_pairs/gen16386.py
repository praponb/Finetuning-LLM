from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"], []],
        F.col("p.region").alias("region"),
        F.col("p.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("src.quantity")), 2).alias("total_quantity"),
        F.grouping_id().alias("level_id"),
    )
)
