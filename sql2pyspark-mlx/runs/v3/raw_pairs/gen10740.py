from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.channel") == "partner")
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.method"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("s.sqft"),
    )
)
