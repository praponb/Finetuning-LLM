from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 194, "high").when(F.col("quantity") >= 63, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.max("last_restock").alias("last_last_restock"),
    )
)
