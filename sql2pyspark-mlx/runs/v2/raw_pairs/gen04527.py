from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("c.credit_limit") > 5800)
    .groupBy(F.col("s.city"))
    .agg(F.count(F.when(F.col("o.channel") == "mobile", 1)).alias("mobile_count"))
    .orderBy("city", F.col("mobile_count").desc())
)
