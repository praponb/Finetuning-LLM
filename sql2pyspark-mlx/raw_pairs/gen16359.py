from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.segment") == "Corporate")
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("c.name"))
    .orderBy(F.col("id").desc())
)
