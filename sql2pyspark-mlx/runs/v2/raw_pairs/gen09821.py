from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 2, 6]) & F.col("product_id").isin([1, 4, 6, 8]) & F.col("store_id").isin([2, 4, 5, 6])))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

summary = (
    valid_inventory
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        (F.max("days_since_last_restock") - F.min("days_since_last_restock")).alias("days_since_last_restock_range"),
    )
)

result = (
    summary
    .select("product_id", "last_restock_month", "days_since_last_restock_range")
    .orderBy("days_since_last_restock_range", "product_id", "last_restock_month")
    .limit(3)
)
