from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.credit_limit") >= 13900) & F.col("s.store_name").like("Ri%"))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.amount"),
        F.col("s.store_name"),
        F.col("c.country"),
        F.col("c.credit_limit"),
    )
)
