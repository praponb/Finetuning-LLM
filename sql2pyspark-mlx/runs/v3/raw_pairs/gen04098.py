from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("s.opened_date")).isin([6, 7, 11]))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("c.signup_date"),
        F.col("c.name"),
        F.col("s.city"),
    )
)
