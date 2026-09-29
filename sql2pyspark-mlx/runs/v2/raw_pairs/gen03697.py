from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 124) & (F.col("quantity").isNotNull() | F.col("product_id").isin([1, 3, 6, 8])))
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)
