from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") < 155) & F.col("store_id").isin([1, 4]) & (F.year("last_restock") == 2025))
    .groupBy(
        F.when(F.col("quantity") >= 266, "high").when(F.col("quantity") >= 68, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
)
