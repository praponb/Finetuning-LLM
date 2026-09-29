from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.customer_id").isin([14, 22]) & (F.length(F.col("s.store_name")) < 10))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy("id", "name")
)
