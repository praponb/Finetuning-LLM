from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.to_date("started_at") < "2023-08-01").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.name"), F.col("c.signup_date"), F.col("c.age"))
)
