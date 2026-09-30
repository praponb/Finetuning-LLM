from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("c.signup_date"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
)
