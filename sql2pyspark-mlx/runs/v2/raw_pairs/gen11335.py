from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("avg_quantity") >= 100)
    .select("store_id", "avg_quantity", "quantity_count")
    .orderBy("quantity_count", F.col("store_id").desc())
    .limit(5)
)
