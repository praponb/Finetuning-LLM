from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3]))
    .groupBy(
        "store_id",
        F.quarter("last_restock").alias("last_restock_quarter"),
        F.when(F.col("quantity") >= 220, "high").when(F.col("quantity") >= 117, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("quantity").alias("min_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 3)
    .drop("unique_products")
    .orderBy("quantity_range")
)
