from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
        "account_id",
    )
    .agg(
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("island_id")).otherwise(0)).alias("fee_island_id"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("merchant") == "Starbucks", 1)), F.lit(0)), 2).alias("island_id_per_starbucks"),
        F.max("txn_time").alias("last_seen"),
        F.percentile_approx("session_start_flag", 0.95).alias("session_start_flag_p95"),
    )
    .filter(F.col("session_start_flag_p95") < 1000000)
)
