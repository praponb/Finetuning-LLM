from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.count(F.when(F.col("o.channel") == "web", 1)).alias("web_count"))
)
