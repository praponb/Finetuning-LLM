from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("age") < 46).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.start_date") >= "2024-09-19")
    .select(F.col("s.id"), F.col("s.monthly_fee"))
    .orderBy(F.col("id").desc(), "monthly_fee")
)
