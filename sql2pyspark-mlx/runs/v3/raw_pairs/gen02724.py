from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.country") == "US") & F.col("c.city").isin(["Chiang Mai", "London", "Singapore"]))
    .select(F.col("p.status"), F.col("o.discount"), F.col("c.age"))
)
