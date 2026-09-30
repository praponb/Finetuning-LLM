from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .groupBy(
        F.when(F.col("quantity") >= 246, "high").when(F.col("quantity") >= 35, "mid").otherwise("low").alias("quantity_bucket"),
        F.year("last_restock").alias("last_restock_year"),
    )
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc(), "quantity_bucket", "last_restock_year")
    .limit(10)
)
