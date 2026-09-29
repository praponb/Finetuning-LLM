from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.max("opened_date").alias("last_opened_date"))
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.name"), F.col("c.segment"), F.col("t.last_opened_date"))
    .orderBy("segment")
)
