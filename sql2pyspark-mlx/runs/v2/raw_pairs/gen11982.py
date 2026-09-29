from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(F.col("a.customer_id"), F.col("A.merchant_list"), F.col("A.stddev_amount"))
)
