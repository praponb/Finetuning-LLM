from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2023-03-01") & F.col("product_id").isin([3, 6, 7, 8]))
    .groupBy(
        F.when(F.col("quantity") >= 222, "high").when(F.col("quantity") >= 91, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
)
