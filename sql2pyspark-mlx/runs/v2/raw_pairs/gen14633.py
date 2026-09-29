from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.month(F.col("customers.signup_date")).alias("signup_month"),
        F.col("customers.segment"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("transactions.txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
    .orderBy("pct_refund")
)
