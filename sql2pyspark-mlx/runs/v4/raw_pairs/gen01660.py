from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 220)
    .orderBy(F.col("max_quantity").desc(), F.col("opened_month").desc())
    .limit(10)
)
