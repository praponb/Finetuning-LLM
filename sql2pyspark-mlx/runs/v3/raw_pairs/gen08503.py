from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 191)
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 221, "high").when(F.col("quantity") >= 88, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .orderBy("quantity_bucket")
)
