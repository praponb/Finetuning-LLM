from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("opened_date").between("2023-03-01", "2024-08-01"))
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.is_active"), F.col("t.balance_range"))
    .orderBy(F.col("id").desc())
)
