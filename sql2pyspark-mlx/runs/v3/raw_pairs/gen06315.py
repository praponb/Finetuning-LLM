from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.credit_amount") >= 70)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("t.credit_amount"),
    )
    .orderBy(F.col("id").desc(), F.col("credit_amount").desc())
)
