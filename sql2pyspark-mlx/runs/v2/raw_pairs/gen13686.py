from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.opened_date") < "2025-07-01") & F.col("c.is_active"))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("c.is_active"),
        F.col("c.age"),
        F.col("s.sqft"),
    )
)
