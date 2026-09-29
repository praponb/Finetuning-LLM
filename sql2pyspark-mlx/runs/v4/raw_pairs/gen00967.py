from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(F.col("c.age"), F.col("a.account_type"), F.col("a.customer_id"))
)
