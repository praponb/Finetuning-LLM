from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5, 6]) & (F.year("last_restock") >= 2025))
)

summary = (
    base
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    summary
    .filter(F.col("quantity_count") > 8)
    .select("last_restock_month", "quantity_count", "stddev_quantity")
)
