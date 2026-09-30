from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.signup_date") >= "2023-04-01")
    .select(F.col("c.id"), F.col("c.segment"))
    .orderBy("segment")
)
