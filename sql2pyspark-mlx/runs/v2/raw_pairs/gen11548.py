from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("s.sqft"),
        F.col("c.credit_limit"),
    )
)
