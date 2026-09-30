from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter((F.upper(F.trim(F.col("s.store_name"))) == "SIAM FLAGSHIP") | F.col("o.status").isin(["RETURNED", "PENDING", "CANCELLED"]))
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("s.region"),
        F.col("c.signup_date"),
        F.datediff(F.current_date(), F.col("o.order_date")).alias("days_since_order_date"),
    )
)
