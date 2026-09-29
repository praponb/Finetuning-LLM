from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(F.countDistinct(F.col("s.cost")).alias("unique_carriers"))
    .orderBy("signup_month", F.col("unique_carriers").desc())
    .limit(3)
)
