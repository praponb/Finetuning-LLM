from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") >= 740)
    .orderBy(F.col("product_id").desc())
)
