from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("s.region") == "Central")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.amount"),
        F.col("c.email"),
        F.col("c.credit_limit"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
    .orderBy("region", F.col("email").desc())
)
