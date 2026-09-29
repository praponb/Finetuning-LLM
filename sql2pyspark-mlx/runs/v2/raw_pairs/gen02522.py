from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.channel").isNull())
    .select(F.col("c.credit_limit"), F.col("o.channel"))
    .orderBy(F.col("credit_limit").desc())
)
