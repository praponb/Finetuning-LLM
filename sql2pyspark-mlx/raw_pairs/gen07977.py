from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Lazada")))
    .select(
        "*",
        F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0).alias("is_7_eleven"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

with_prev = cleaned.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

kept_keys = (
    sessionized
    .select("txn_type", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("amount") < 1400)
        .select("txn_type", "account_id")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance").between(23000, 77000))
    .select("s.*", F.col("a.status"), F.col("a.balance"))
)

cleaned_transactions = (
    joined
    .filter(F.col("is_new_burst").isNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_7_eleven",
        "prev_txn_time",
        "is_new_burst",
        "session_no",
        "balance",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)

enriched = (
    cleaned_transactions
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .select("src.*", F.col("a.account_type"), F.col("a.status"))
)

joined_transactions = (
    enriched
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.amount",
        "s.txn_time",
        "s.merchant",
        "s.is_new_burst",
        "s.session_no",
        "s.balance",
        "s.balance_filled",
        "s.status",
        F.col("d.account_type").alias("account_account_type"),
        F.col("d.opened_date"),
    )
)

result = (
    joined_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .cube(
        F.col("d.account_type").alias("account_type"),
        F.col("d.status").alias("account_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("e.balance")), 2).alias("total_balance"),
        F.round(F.avg(F.col("e.session_no")), 2).alias("avg_session_no"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("total_balance").asc_nulls_last())
)
