from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(F.date_trunc("month", "txn_time").alias("activity_month"), "txn_type")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("txn_time").alias("last_txn_time"),
    )
)
