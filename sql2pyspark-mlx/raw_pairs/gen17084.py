from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("account_type", "customer_id")
w4 = Window.partitionBy("account_type", "txn_type").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("customer_id", "transaction_merchant").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("txn_type").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("transaction_txn_type").orderBy("transaction_amount", "id")

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-03-13")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_sub("opened_date", 69).alias("opened_date_minus_69d"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        F.col("c.txn_type"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

scored = (
    with_last
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("account_type_count_balance", F.count("balance").over(w3))
    .withColumn("balance_avg_2", F.avg("balance").over(w4))
    .withColumn("balance_avg_5", F.avg("balance").over(w5))
)

latest_match = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

latest_match_accounts = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"))
)

enriched = (
    latest_match_accounts
    .filter(F.col("account_type_count_balance").isNotNull())
    .select(
        "*",
        F.sum("balance_avg_5").over(w_running).alias("running_sum_balance_avg_5"),
        F.count("balance_avg_5").over(w_running).alias("running_count_balance_avg_5"),
        F.max("balance_avg_5").over(w_running).alias("running_max_balance_avg_5"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

result = (
    enriched
    .groupBy("seq_no")
    .agg(
        F.count_if(F.col("transaction_merchant") == "7-Eleven").alias("7_eleven_events"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("transaction_merchant")).alias("transaction_merchant_list"),
        F.percentile_approx("running_sum_balance_avg_5", [0.25, 0.5, 0.75]).alias("running_sum_balance_avg_5_quartiles"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("balance"))).alias("debit_total_balance"),
        F.min("event_seq").alias("min_event_seq"),
    )
    .filter(F.col("debit_total_balance") < 1000000)
    .orderBy(F.col("debit_total_balance").desc())
)
