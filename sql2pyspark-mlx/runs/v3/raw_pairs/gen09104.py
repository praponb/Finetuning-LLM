from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "REFUND"]))
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(F.col("a.customer_id"), F.col("a.opened_date"), F.col("as.txn_type_values"))
)
