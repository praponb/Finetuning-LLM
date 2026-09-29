from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(F.col("o.amount"), F.col("c.country"), F.col("c.city"))
    .orderBy(F.col("city").desc())
)
