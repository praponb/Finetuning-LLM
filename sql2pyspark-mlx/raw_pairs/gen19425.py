from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_time",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

result = (
    sessions_transactions
    .withColumn("quarter_start", F.date_trunc("QUARTER", "txn_time"))
    .groupingSets(
        [["account_id", "quarter_start"], ["account_id"], []],
        "account_id",
        "quarter_start",
    )
    .agg(
        F.min("txn_time").alias("started_at"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.min("txn_time").alias("started_at_2"),
        F.max("txn_time").alias("ended_at"),
        F.max("txn_time").alias("last_txn_time"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.grouping("account_id").alias("gid"),
    )
    .orderBy(F.col("account_id").desc_nulls_last())
)
