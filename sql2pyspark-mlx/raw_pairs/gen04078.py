from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .groupBy("account_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("num_transactions"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.cnt") >= 7)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("at.cnt"),
        F.col("at.num_transactions"),
    )
    .orderBy(F.col("status").desc())
)
