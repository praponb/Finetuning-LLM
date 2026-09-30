from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        "store_id",
        F.when(F.col("quantity") >= 190, "high").when(F.col("quantity") >= 61, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("quantity_bucket")
)
