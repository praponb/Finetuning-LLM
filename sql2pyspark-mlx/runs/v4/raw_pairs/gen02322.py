from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 10)
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") < 210)
    .drop("avg_quantity")
)
