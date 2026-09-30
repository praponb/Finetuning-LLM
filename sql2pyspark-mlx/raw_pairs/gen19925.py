from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date") <= "2025-07-01")
    .select(F.col("o.discount"), F.col("c.name"), F.col("s.region"), F.col("s.city"))
    .orderBy("name", F.col("city").desc())
)
