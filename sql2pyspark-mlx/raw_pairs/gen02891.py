from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

valid = (
    islands
    .filter((F.col("txn_time") >= F.lit("2023-10-09").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "session_start_flag",
        "burst_id",
        F.length("merchant").alias("merchant_length"),
        (F.col("session_start_flag") + F.col("amount")).alias("session_start_flag_plus_amount"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

with_gaps_2 = (
    sessions_transactions
    .withColumn("session_start_flag_2", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps_2
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "session_start_flag",
        "merchant_length",
        "merchant_clean",
        "is_new_session",
        "session_no",
        F.sum("session_start_flag_2").over(w2).alias("burst_id_2"),
    )
)

result = (
    sessionized
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant_length",
        "merchant_clean",
        "is_new_session",
        "session_no",
        "burst_id_2",
    )
)
