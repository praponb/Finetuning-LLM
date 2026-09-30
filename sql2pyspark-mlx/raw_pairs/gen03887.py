from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type").isin(["DEBIT", "CREDIT", "REFUND"])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.balance") < 38000)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("a.customer_id"),
    )
    .orderBy(F.col("status").desc(), "id")
    .limit(5)
)
