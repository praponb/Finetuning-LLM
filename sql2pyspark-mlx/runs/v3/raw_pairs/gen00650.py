from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("customer_id").isin([1, 10])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.country") == "JP")
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("c.is_active"))
    .orderBy("signup_date")
)
