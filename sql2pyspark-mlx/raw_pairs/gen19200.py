from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.lower("txn_type").alias("txn_type_lower"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    gaps
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "txn_type_lower",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

gaps_2 = (
    sessions_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps_2.withColumn("burst_id_2", F.sum("session_start_flag").over(w2))

flagged = (
    islands
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions_2 = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions_2
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "txn_type_lower",
        "burst_id",
        "session_start_flag",
        "burst_id_2",
        F.when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "PRIORITY").otherwise("GROUP_A").alias("txn_type_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last(), "txn_type_zone")
    .limit(5)
)
