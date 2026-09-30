from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.col("s.opened_date") < "2025-07-01")
    .groupBy(F.col("s.region"))
    .agg(
        F.round(F.stddev(F.col("o.amount")), 2).alias("stddev_amount"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
    .filter(F.col("stddev_amount") >= 35)
    .orderBy("stddev_amount")
)
