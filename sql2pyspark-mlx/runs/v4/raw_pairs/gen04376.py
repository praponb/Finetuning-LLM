from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-07-05 09:00:00")))
    .select(
        "*",
        F.when(F.col("merchant").isin(["Starbucks", "Grab"]), "Priority").otherwise("Other").alias("merchant_group"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .cube(
        "island_id",
        F.when(F.col("amount") >= 4200, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("session_start_flag", 0.95).alias("session_start_flag_p95"),
        F.sum("session_start_flag").alias("total_session_start_flag"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("session_start_flag")).otherwise(0)).alias("starbucks_session_start_flag"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_rows"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
)
