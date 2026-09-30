from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.balance_range") >= 35)
    .select(F.col("c.id"), F.col("c.country"), F.col("t.balance_range"))
    .orderBy(F.col("country").desc(), "id")
    .limit(5)
)
