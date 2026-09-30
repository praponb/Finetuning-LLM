from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("burst_id") >= 7, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
    )
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
    )
    .filter(F.col("num_txn_types") > 1)
)
