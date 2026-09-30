from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"), F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.count("*").alias("num_order_items"))
    .filter(F.col("num_order_items") >= 2)
    .orderBy("segment")
)
