from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.email").like("%@company.co"))
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("c.age"),
        F.col("c.email"),
        (F.col("o.amount") * F.col("o.discount")).alias("amount_times_discount"),
    )
)
