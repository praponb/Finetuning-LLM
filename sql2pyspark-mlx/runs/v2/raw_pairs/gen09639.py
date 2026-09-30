from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("o.channel") != "web") & (F.upper(F.trim(F.col("c.name"))) == "KANYA SRISUK"))
    .select(F.col("c.country"), F.col("o.store_id"))
    .orderBy("store_id", "country")
)
