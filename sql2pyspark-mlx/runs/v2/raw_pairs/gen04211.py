from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.avg("balance").alias("avg_balance"))
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("A"), F.col("c.id") == F.col("A.customer_id"), "left")
    .select(F.col("c.segment"), F.col("c.is_active"), F.col("A.avg_balance"))
    .orderBy(F.col("segment").desc())
)
