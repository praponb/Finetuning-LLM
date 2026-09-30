from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("num_transactions"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .filter(F.col("A.num_transactions") >= 7)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("A.stddev_amount"),
        F.col("A.num_transactions"),
    )
    .orderBy(F.col("num_transactions").desc())
)
