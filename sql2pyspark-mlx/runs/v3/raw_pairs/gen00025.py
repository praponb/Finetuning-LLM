from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.debit_amount") >= 66)
    .select(
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("tba.debit_amount"),
        F.col("tba.merchant_list"),
    )
    .orderBy(F.col("customer_id").desc())
)
