from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.channel").isin(["store", "mobile", "web"]) & (F.col("o.store_id") == 30))
    .select(F.col("o.id"), F.col("o.store_id"), F.col("c.email"))
    .orderBy("store_id", "email")
)
