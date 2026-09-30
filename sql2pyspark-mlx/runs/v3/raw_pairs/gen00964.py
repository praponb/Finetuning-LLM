from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("at.amount_count"),
        F.col("at.txn_type_values"),
    )
)
