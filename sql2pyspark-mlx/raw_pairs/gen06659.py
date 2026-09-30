from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.balance") <= 77000)
    .select(F.col("a.id"), F.col("a.status"), F.col("c.city"), F.col("c.segment"))
)
