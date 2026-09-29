from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5]) & F.col("store_id").isin([2, 4, 5, 6]))
)

quantity_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 264, "high").when(F.col("quantity") >= 97, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    quantity_bucket_summary
    .orderBy(F.col("max_quantity").desc(), F.col("store_id").desc())
)
