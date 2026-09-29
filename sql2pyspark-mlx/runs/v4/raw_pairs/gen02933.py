from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.count("*").alias("cnt"),
    )
)
