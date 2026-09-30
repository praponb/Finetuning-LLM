from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]))
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.pct_refund") > 67)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("at.txn_type_list"),
        F.col("at.pct_refund"),
    )
    .orderBy(F.col("customer_id").desc())
)
