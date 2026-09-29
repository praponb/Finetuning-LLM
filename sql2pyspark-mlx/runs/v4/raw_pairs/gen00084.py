from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.segment").isNotNull() & F.col("o.amount").between(1280, 1350))
    .select(
        F.col("o.customer_id"),
        F.col("s.sqft"),
        F.col("c.signup_date"),
        F.col("c.age"),
    )
)
