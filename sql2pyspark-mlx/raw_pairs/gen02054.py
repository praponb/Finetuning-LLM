from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("session_start_flag", "id")

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

valid = (
    islands
    .filter(F.col("session_no").between(2, 3))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "session_start_flag",
        "session_no",
        F.ceil("session_no").alias("session_no_whole"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    deduped
    .select("txn_type")
    .intersect(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("txn_type")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("txn_time") >= F.lit("2023-05-30").cast("date"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "session_start_flag",
        "session_no",
        "session_no_whole",
        "dedup_rank",
        (F.col("session_no") + F.col("session_start_flag")).alias("session_no_plus_session_start_flag"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.balance"), F.col("d.account_type"))
)

enriched_transactions = (
    enriched
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.account_type").alias("account_account_type"))
)

daily = (
    enriched_transactions
    .groupBy("account_account_type")
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
    )
    .filter(F.col("num_account_types") < 1000)
)

result = daily.select("account_account_type", "num_account_types")
