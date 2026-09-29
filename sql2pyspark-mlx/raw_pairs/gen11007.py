from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("year_start").orderBy(F.col("event_count").desc())

ranked = (
    df_accounts
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

eligible = (
    ranked
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.account_id").isin([8, 22, 30, 31])), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

eligible_accounts = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.is_flagged")), "left_anti")
)

latest_match_accounts = (
    eligible_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.balance", F.col("c.txn_time").alias("transaction_txn_time"))
)

cleaned = (
    latest_match_accounts
    .filter(F.col("transaction_txn_time") >= "2024-11-01")
    .select(
        "*",
        F.datediff(F.current_date(), "transaction_txn_time").alias("days_since_transaction_txn_time"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("year", "transaction_txn_time").alias("year_start"),
        F.when(F.col("balance") >= 72000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        F.when(F.col("days_since_transaction_txn_time") >= 77, "LARGE").otherwise("SMALL").alias("days_since_transaction_txn_time_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.min("transaction_txn_time").alias("first_seen"),
        F.min("days_since_transaction_txn_time").alias("min_days_since_transaction_txn_time"),
    )
    .filter(F.col("min_days_since_transaction_txn_time") < 1000000)
    .withColumn("event_count_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("event_count").desc())
)
