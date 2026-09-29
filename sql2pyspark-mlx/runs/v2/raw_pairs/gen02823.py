from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .filter(F.col("a.customer_id").isNotNull())
    .join(df_customers.filter(F.year("signup_date") == 2024).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("a.status"))
)
