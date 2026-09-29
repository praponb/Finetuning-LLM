from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter((F.year("last_restock") >= 2023) & (F.col("last_restock") < "2024-10-01"))
)

summary = (
    filtered_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    summary
    .filter(F.col("stddev_quantity") >= 50)
    .select("last_restock_month", "stddev_quantity")
    .orderBy(F.col("last_restock_month").desc())
)
