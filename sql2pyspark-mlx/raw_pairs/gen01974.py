from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.order_date") <= "2024-10-01")
    .select(
        F.col("p.method"),
        F.col("o.order_date"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.to_date(F.col("p.paid_at")).alias("paid_at_date"),
    )
    .orderBy("paid_at_date")
)
