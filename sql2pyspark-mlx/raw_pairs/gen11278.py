from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-08-07"))) & F.col("merchant").isNotNull())
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("island_id") >= 8, "HIGH").otherwise("LOW").alias("island_id_tier"),
        F.date_trunc("quarter", "txn_time").alias("quarter_start"),
        F.when(F.col("amount") >= 4200, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.round(F.avg("is_flagged_flag"), 2).alias("avg_is_flagged_flag"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.percentile_approx("is_flagged_flag", 0.5).alias("is_flagged_flag_p50"),
        F.round(F.sum("is_flagged_flag") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("is_flagged_flag_per_debit"),
        F.min("session_start_flag").alias("min_session_start_flag"),
    )
    .orderBy("avg_is_flagged_flag")
)
