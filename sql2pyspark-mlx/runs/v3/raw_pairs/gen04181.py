from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.customer_id"),
        F.col("s.store_name"),
        F.col("c.city"),
        F.col("c.credit_limit"),
    )
)
