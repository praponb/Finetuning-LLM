from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("c.city"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy("id", F.col("city").desc())
)
