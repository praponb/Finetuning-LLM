from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("quantity").isNotNull() & (F.col("store_id").isin([3, 6]) | (F.col("last_restock") > "2025-07-01")))
)

agg = (
    valid_inventory
    .groupBy(
        F.when(F.col("quantity") >= 193, "high").when(F.col("quantity") >= 97, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    agg
    .filter(F.col("avg_quantity") < 44)
    .select("quantity_bucket", "avg_quantity")
    .orderBy("quantity_bucket")
    .limit(50)
)
