from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("credit_limit") >= 8300).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.start_date") > "2023-04-01")
    .select(F.col("s.id"), F.col("s.start_date"))
)
