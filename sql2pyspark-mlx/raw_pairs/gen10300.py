from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.max("balance").alias("max_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.max_balance") > 52000)
    .select(
        F.col("c.name"),
        F.col("c.credit_limit"),
        F.col("C.avg_balance"),
        F.col("C.max_balance"),
    )
)
