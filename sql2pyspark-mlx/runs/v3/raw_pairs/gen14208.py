from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.status"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
    )
    .filter(F.col("cnt") >= 5)
    .orderBy("stddev_amount", "status")
    .limit(1)
)
