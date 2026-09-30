from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("c.country") != "JP")
    .select(
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
)
