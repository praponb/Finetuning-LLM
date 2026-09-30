from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.status").isin(["SHIPPED", "COMPLETED"]))
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(F.count(F.col("p.amount")).alias("amount_count"))
    .orderBy(F.col("amount_count").desc())
)
