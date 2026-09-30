from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
    .orderBy(F.col("store_id").desc())
)
