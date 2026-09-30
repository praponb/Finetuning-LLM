from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(~((F.col("s.sqft") != 14248) | (F.col("s.city") == "Berlin")))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("s.region"),
    )
)
