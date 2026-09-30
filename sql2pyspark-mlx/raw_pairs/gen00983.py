from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 232, "high").when(F.col("quantity") >= 79, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("unique_products") > 7)
    .select("quantity_bucket", "product_id", "unique_products", "min_quantity")
    .orderBy("quantity_bucket")
)
