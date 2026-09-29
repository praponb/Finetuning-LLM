from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.amount").between(1020, 1080))
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("c.country"),
    )
)
