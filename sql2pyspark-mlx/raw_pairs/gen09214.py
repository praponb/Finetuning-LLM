from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "outer")
    .filter(F.col("t.txn_type").isin(["REFUND", "FEE"]))
    .groupBy(F.col("a.account_type"))
    .agg(
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.merchant")))).alias("merchant_values"),
    )
)
