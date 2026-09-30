from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("avg_quantity") < 190)
    .orderBy(F.col("product_id").desc())
)
