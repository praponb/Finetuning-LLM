from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
    )
    .orderBy("opened_month", F.col("amount_range").desc())
)
