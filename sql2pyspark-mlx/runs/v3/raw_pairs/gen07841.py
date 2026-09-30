from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.amount").between(510, 940))
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.max(F.col("p.paid_at")).alias("latest_paid_at"))
)
