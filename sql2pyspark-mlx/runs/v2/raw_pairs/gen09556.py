from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.method") != "card")
    .groupBy(F.col("o.store_id"))
    .agg(F.max(F.col("p.paid_at")).alias("last_paid_at"))
    .orderBy("store_id")
    .limit(50)
)
