from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.opened_date") <= "2024-03-01") | F.col("o.channel").isin(["partner", "store", "web"]))
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("s.store_name"),
        F.substring(F.col("o.channel"), 1, 3).alias("channel_prefix"),
    )
)
