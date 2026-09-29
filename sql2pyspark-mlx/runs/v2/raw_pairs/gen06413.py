from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.col("o.store_id").isin([14, 19, 23, 39]))
    .groupBy(F.col("o.store_id"))
    .agg(
        F.min(F.col("p.amount")).alias("min_amount"),
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
    )
    .orderBy(F.col("min_amount").desc(), "store_id")
    .limit(50)
)
