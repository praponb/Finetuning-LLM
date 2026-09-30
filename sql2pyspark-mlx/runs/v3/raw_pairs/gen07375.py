from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .groupBy(
        F.when(F.col("quantity") >= 268, "high").when(F.col("quantity") >= 61, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
