from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Berlin")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
        F.max(F.col("p.paid_at")).alias("last_paid_at"),
    )
    .filter(F.col("pct_paypal") > 40)
)
