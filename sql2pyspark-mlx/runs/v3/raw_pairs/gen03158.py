from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .cube("store_id", "product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.count("*").alias("total_count"),
    )
)
