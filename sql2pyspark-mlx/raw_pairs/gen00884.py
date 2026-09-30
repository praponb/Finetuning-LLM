from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 210, "high").when(F.col("quantity") >= 48, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") >= 5)
    .select("quantity_bucket", "unique_products")
    .orderBy(F.col("unique_products").desc())
)
