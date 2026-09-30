from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(F.date_trunc("week", "txn_time").alias("week_start"))
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("merchant") == "Starbucks", 1)), F.lit(0)), 2).alias("session_start_flag_per_starbucks"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
    )
    .orderBy(F.col("fee_rows").desc_nulls_last())
)
