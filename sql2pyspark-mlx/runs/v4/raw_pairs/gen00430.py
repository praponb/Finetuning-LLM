from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("amount") >= 4200, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.max(F.when(F.col("txn_type") == "CREDIT", F.col("amount"))).alias("credit_max_amount"),
        F.percentile_approx("is_new_burst", 0.5).alias("is_new_burst_p50"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.avg(F.when(F.col("txn_type") == "CREDIT", F.col("burst_id"))).alias("credit_avg_burst_id"),
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
    )
    .filter(F.col("is_new_burst_p50") > 0)
)
