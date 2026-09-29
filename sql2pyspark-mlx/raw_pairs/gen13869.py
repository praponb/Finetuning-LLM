from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_transactions
    .groupBy(
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        F.when(F.col("burst_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("burst_id_tier").asc_nulls_first())
)
