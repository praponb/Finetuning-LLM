from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("c.email"),
    )
    .orderBy(F.col("store_id").desc())
)
