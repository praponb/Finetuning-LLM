from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("session_id", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy(F.year("txn_time"), "txn_type")
w6 = Window.partitionBy("txn_type").orderBy(F.col("session_id").desc())
w7 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.partitionBy("txn_type")

cleaned = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("txn_type").isin(["REFUND", "DEBIT"]), "Group A").otherwise("Other").alias("txn_type_group"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

base = (
    sessionized
    .filter(F.col("session_id").between(4, 7))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "session_id",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.round("session_id", 0).alias("session_id_rounded"),
    )
)

metrics = (
    base
    .filter(F.col("days_since_txn_time") != 32)
    .withColumn("amount_avg_2", F.avg("amount").over(w3))
    .withColumn("amount_avg_90d", F.avg("amount").over(w4))
)

remaining_keys = (
    metrics
    .select("id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("amount") < 1400)
        .select("id", "txn_type")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w5)), 4))
)

metrics_transactions = (
    scored
    .select(
        "*",
        F.percent_rank().over(w6).alias("session_id_percent_rank"),
        F.max("amount_avg_2").over(w7).alias("amount_avg_2_max_30d"),
        F.round((F.col("amount_avg_2") / F.sum("amount_avg_2").over(w8)), 4).alias("amount_avg_2_share"),
    )
)

result = (
    metrics_transactions
    .cube("txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows"),
        F.round(F.sum("amount_avg_2") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("amount_avg_2_per_debit"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .orderBy(F.col("amount_avg_2_per_debit").asc_nulls_first())
)
