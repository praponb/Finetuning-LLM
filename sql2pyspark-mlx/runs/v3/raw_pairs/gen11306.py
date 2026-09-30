from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.order_id").isin([22, 24, 27, 39]))
    .select(
        F.col("s.shipped_date"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("c.signup_date"),
        F.col("c.name"),
    )
)
