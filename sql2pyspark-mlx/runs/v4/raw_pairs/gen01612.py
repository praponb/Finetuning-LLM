from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .select("id", "account_id", "amount", "merchant", "is_flagged")
    .orderBy(F.col("account_id").asc_nulls_last(), "amount")
)
