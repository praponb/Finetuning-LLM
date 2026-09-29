from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 198, "high").when(F.col("quantity") >= 32, "mid").otherwise("low").alias("quantity_bucket"),
        F.month("last_restock").alias("last_restock_month"),
    )
    .agg(F.max("last_restock").alias("last_last_restock"))
)
