from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-03-24")
    .groupBy(
        F.when(F.col("quantity") >= 216, "high").when(F.col("quantity") >= 35, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") < 4)
    .select("quantity_bucket", "quantity_count", "unique_products")
)
