from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.channel") != "web")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("c.is_active"), F.col("c.name"))
    .orderBy("name", F.col("store_id").desc())
)
