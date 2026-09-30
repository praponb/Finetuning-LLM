from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.avg_amount") > 3600)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("at.avg_amount"),
        F.col("at.txn_type_values"),
    )
    .orderBy("id")
)
