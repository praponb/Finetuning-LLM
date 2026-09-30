from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .cube(F.date_trunc("MONTH", "txn_time").alias("activity_month"))
    .agg(
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("new_group_flag_per_refund"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
    )
)
