from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("end_date") < "2023-09-01").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.signup_date"))
)
