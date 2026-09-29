from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 208, "high").when(F.col("quantity") >= 88, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") < 9)
    .select("quantity_bucket", "product_id", "unique_products")
)
