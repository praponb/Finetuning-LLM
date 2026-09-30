from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.segment"), F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.count("*").alias("account_count"))
)
