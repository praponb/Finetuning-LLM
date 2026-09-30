from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.min("balance").alias("min_balance"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.checking_balance") >= 55)
    .select(
        F.col("customers.country"),
        F.col("customers.credit_limit"),
        F.col("t.checking_balance"),
        F.col("t.min_balance"),
    )
)
