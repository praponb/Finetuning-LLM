from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .select(F.col("accounts.balance"), F.col("customers.city"))
    .orderBy(F.col("city").desc(), "balance")
)
