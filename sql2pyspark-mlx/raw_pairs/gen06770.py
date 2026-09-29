from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(~F.col("txn_type").isin(["CREDIT", "FEE", "DEBIT"]))
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("tba.pct_lazada"),
        F.col("tba.amount_count"),
    )
    .orderBy(F.col("id").desc(), "amount_count")
    .limit(5)
)
