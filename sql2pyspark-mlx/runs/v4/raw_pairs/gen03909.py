from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("t.txn_type") == "REFUND")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.merchant")))).alias("merchant_values"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
    )
    .orderBy("merchant_values", "signup_month")
    .limit(5)
)
