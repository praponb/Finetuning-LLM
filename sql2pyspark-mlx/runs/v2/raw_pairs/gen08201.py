from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9, 10]))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.col("min_quantity").desc())
)
