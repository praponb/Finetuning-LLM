from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.sqft") != 10426) | (F.col("s.sqft") >= 12479))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("s.sqft"),
        F.upper(F.col("o.status")).alias("status_upper"),
    )
)
