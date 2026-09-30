from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("a.customer_id"))
    .agg(
        F.avg(F.col("t.amount")).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.txn_type")))).alias("txn_type_values"),
    )
    .orderBy(F.col("txn_type_values").desc(), "customer_id")
    .limit(5)
)
