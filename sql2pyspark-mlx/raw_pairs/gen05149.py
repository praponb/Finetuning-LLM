from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .filter(F.col("avg_quantity") < 49)
    .select("store_id", "avg_quantity", "last_last_restock")
    .orderBy("store_id", "avg_quantity")
)
