from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.segment"), F.col("c.is_active"), F.col("cs.credit_count"))
    .orderBy(F.col("credit_count").desc())
)
