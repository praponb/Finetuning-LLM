from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_no_tier").orderBy(F.col("amount_p50").desc())

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_no") >= 9, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
    )
    .agg(
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("session_start_flag_per_debit"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("session_start_flag")).otherwise(0)).alias("refund_session_start_flag"),
        F.max("txn_time").alias("ended_at"),
        F.max("txn_time").alias("last_txn_time"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
    .withColumn("amount_p50_rank", F.rank().over(w3))
)
