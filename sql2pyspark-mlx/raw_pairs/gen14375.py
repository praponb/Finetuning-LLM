from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.status").isin(["RETURNED", "CANCELLED"]))
    .groupBy(F.col("s.city"))
    .agg(F.count(F.col("p.amount")).alias("amount_count"))
    .orderBy(F.col("city").desc())
)
