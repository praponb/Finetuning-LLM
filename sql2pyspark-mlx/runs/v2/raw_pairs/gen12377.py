from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") >= 840)
    .select("store_id", "avg_quantity", "total_quantity")
    .orderBy(F.col("store_id").desc())
    .limit(3)
)
