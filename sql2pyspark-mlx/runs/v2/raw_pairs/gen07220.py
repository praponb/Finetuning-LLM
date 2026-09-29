from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"))
    .orderBy("stddev_amount", "opened_month")
    .limit(50)
)
