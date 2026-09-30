from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .groupBy("session_id")
    .agg(
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Shell", 1)), F.lit(0)), 2).alias("amount_per_shell"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("amount_per_fee"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
)
