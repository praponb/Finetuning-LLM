from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(~F.col("t.is_flagged"))
    .groupBy(F.col("a.customer_id"), F.col("a.account_type"))
    .agg(F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"))
    .orderBy("customer_id")
)
