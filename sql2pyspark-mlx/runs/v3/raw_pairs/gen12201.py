from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS"]))
    .groupBy("customer_id")
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .select(F.col("customers.id"), F.col("customers.age"), F.col("t.min_balance"))
)
