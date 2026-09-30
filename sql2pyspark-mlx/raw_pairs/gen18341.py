from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")

valid = (
    df_transactions
    .filter(F.col("amount") <= 1200)
    .select(
        "*",
        F.concat("merchant", F.lit(" - "), "txn_type").alias("merchant_txn_type"),
    )
)

enriched = (
    valid
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.account_type") == "SAVINGS")
    .select("a.*", F.col("d.balance"), F.col("d.opened_date"), F.col("d.account_type"))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .withColumn("prev_balance", F.lag("balance").over(w2))
    .withColumn("next_amount", F.lead("amount").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("next_balance", F.lead("balance", 1).over(w3))
)

result = (
    metrics
    .groupBy("merchant", F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.max("txn_time").alias("ended_at"),
        F.max(F.when(F.col("account_type") == "LOAN", F.col("balance"))).alias("loan_max_balance"),
        F.count("*").alias("row_count"),
        F.min("txn_time").alias("started_at"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("activity_month").desc())
)
