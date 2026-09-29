from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 6)
    .orderBy(F.col("product_id").desc(), F.col("quantity_count").desc())
)
