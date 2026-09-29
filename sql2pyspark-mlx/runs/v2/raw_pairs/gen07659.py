from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([5, 8, 11]))
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 3)
    .drop("quantity_count")
    .orderBy(F.col("product_id").desc())
)
