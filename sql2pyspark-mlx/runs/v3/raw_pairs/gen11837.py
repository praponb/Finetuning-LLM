from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(F.count(F.col("p.amount")).alias("amount_count"))
    .orderBy(F.col("amount_count").desc())
)
