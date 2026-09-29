from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("last_restock").between("2024-08-27", "2025-09-01") & (F.col("last_restock") < "2024-04-01")))
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") < 55)
)
