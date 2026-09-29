from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "inner")
    .filter(F.col("account_stats.stddev_amount") >= 20)
    .select(
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("account_stats.merchant_values"),
        F.col("account_stats.stddev_amount"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
