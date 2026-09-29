from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.discount"),
        F.col("s.region"),
        F.col("c.age"),
        F.col("c.country"),
        (F.col("c.age") + F.col("c.credit_limit")).alias("age_plus_credit_limit"),
    )
)
