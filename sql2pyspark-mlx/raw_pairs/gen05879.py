from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5, 6, 9]))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
