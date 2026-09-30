from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.max(F.when(F.col("merchant") == "Grab", F.col("session_start_flag"))).alias("grab_max_session_start_flag"),
        F.count_if(F.col("merchant") == "Grab").alias("grab_events"),
    )
    .filter(F.col("grab_events") >= 1)
    .orderBy(F.col("quarter_start").desc())
)
