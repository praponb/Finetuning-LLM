from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 1)
    .orderBy(F.col("first_last_restock").desc())
)
