from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]) & F.col("store_id").isin([1, 3, 4]))
    .groupBy(
        F.when(F.col("quantity") >= 183, "high").when(F.col("quantity") >= 68, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.min("quantity").alias("min_quantity"))
    .orderBy(F.col("quantity_bucket").desc(), F.col("product_id").desc())
)
