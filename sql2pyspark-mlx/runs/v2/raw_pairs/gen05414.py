from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 236, "high").when(F.col("quantity") >= 88, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.min("quantity").alias("min_quantity"),
    )
)
