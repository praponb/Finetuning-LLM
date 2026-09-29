from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("tba.unique_merchants"),
        F.col("tba.stddev_amount"),
    )
    .orderBy("customer_id")
)
