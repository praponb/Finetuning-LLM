from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.quantity").between(98, 104))
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy("total_count", "stddev_quantity")
)
