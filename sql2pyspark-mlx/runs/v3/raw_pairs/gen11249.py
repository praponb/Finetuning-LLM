from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("balance") >= 48000)
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.col("abc.frozen_balance"),
        F.col("abc.avg_balance"),
    )
    .orderBy("frozen_balance")
)
