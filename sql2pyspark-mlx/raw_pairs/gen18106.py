from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 6, 9, 10]) | (F.col("last_restock") > "2025-09-06"))
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 239, "high").when(F.col("quantity") >= 58, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("unique_stores") < 4)
    .orderBy(F.col("store_id").desc())
)
