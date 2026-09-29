from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("s.region"),
        F.col("c.age"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("id").desc(), F.col("age").desc())
)
