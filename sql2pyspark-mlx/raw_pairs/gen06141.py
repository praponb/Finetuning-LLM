from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") < 51)
    .select("product_id", "last_last_restock", "avg_quantity")
    .orderBy("last_last_restock")
)
