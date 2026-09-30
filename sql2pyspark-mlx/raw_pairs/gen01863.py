from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-06-06 16:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.to_date("txn_time").alias("txn_time_date"),
    )
)

cleaned = (
    valid
    .filter(F.col("merchant").isNotNull() & (F.col("merchant") == "Lazada"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "txn_time_date",
        (F.datediff(F.current_date(), "txn_time") / 30).alias("months_since_txn_time"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "txn_time_date",
        "months_since_txn_time",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

with_gaps = (
    sessions_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "merchant",
        F.col("amount").alias("AMOUNT"),
        F.col("months_since_txn_time").alias("MONTHS_SINCE_TXN_TIME"),
    )
    .unpivot(
        ids=["id", "merchant"],
        values=["AMOUNT", "MONTHS_SINCE_TXN_TIME"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
