from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "DEBIT", "CREDIT"]))
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.total_amount") >= 15000)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("as.merchant_values"),
        F.col("as.total_amount"),
    )
)
