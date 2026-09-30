from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("amount") < 3300)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.substring("txn_type", 1, 3).alias("txn_type_prefix"),
        F.length("merchant").alias("merchant_length"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy("merchant")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("session_start_flag_per_debit"),
        F.min("txn_time").alias("first_txn_time"),
        F.percentile_approx("merchant_length", [0.25, 0.5, 0.75]).alias("merchant_length_quartiles"),
        F.round(F.sum("merchant_length") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("merchant_length_per_refund"),
    )
    .orderBy(F.col("cnt").desc())
)
