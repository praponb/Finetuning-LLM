from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.discount"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
    .orderBy("credit_limit")
)
