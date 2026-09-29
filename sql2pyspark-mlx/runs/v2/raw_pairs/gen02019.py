from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("balance").between(44000, 71000))
    .groupBy("customer_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "inner")
    .select(F.col("c.name"), F.col("c.segment"), F.col("abc.cnt"))
)
