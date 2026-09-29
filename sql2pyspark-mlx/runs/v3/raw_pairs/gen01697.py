from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "outer")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("accounts.customer_id"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("transactions.txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .filter(F.col("pct_credit") >= 37)
    .orderBy("pct_credit")
)
