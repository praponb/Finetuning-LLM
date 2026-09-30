from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("o.discount"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
        F.col("s.store_name"),
        F.col("s.region"),
    )
)
