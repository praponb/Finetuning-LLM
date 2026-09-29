from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.length(F.col("c.name")) > 7) | (F.col("c.signup_date") <= "2024-03-01"))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("c.name"),
        F.col("s.region"),
    )
)
