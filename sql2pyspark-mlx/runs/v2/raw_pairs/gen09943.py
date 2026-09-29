from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([2, 10, 11]))
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("s.city"), F.col("c.age"))
)
