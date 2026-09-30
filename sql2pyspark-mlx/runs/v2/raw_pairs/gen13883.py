from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.channel").isin(["web", "mobile"]))
    .select(F.col("c.credit_limit"), F.col("c.is_active"), F.col("o.shipping_fee"))
)
