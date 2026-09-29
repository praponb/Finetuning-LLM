from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc(), "last_restock_month")
    .limit(3)
)
