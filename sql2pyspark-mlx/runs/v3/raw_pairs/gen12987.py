from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("a.balance") >= 88000, "high").when(F.col("a.balance") >= 10000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.txn_type")))).alias("txn_type_values"),
    )
    .orderBy("txn_type_values")
)
