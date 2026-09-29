from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.last_restock") >= "2024-08-22")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
)
