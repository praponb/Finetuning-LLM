from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.month(F.col("a.opened_date")).isin([4, 5, 9]))
    .groupBy(F.col("a.account_type"), F.col("a.status"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.txn_type")))).alias("txn_type_values"),
        F.sum(F.when(F.col("t.merchant") == "Grab", F.col("t.amount")).otherwise(0)).alias("grab_amount"),
    )
    .orderBy("status", "account_type")
    .limit(20)
)
