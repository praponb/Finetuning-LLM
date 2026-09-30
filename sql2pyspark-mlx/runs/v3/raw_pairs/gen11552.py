from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.sqft") >= 17570, "high").when(F.col("s.sqft") >= 4121, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
)
