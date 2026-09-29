from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.city"), F.col("abc.min_balance"))
    .orderBy("min_balance", "city", "id")
    .limit(20)
)
