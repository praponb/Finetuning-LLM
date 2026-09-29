from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy("txn_type")
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("txn_type").desc())
)
