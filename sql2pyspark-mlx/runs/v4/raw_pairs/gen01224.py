from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "txn_time"))
    .groupingSets(
        [["activity_month", "quarter_start"], ["activity_month"]],
        "activity_month",
        "quarter_start",
    )
    .agg(
        F.min("txn_time").alias("first_seen"),
        F.sum("amount").alias("total_amount"),
        F.max("txn_time").alias("last_txn_time"),
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.grouping("activity_month").alias("grouping_level"),
    )
)
