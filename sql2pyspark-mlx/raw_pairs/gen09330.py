from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "FEE", "REFUND"]))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.lazada_amount") > 29)
    .select(F.col("a.account_type"), F.col("tba.lazada_amount"), F.col("tba.pct_grab"))
    .orderBy(F.col("pct_grab").desc())
)
