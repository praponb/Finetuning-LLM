from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(~F.col("c.email").like("%@startup.io"))
    .select(F.col("o.channel"), F.col("o.customer_id"), F.col("c.credit_limit"))
    .orderBy("credit_limit")
)
