from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("txn_time"), "merchant")
w4 = Window.partitionBy(F.date_trunc("WEEK", "txn_time"), "account_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy("opened_date", "id")

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-10-24").cast("date"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("years_since_txn_time", (F.datediff(F.current_date(), "txn_time") / 365))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("UNKNOWN")))
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

joined = (
    sessions_transactions
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .filter(~F.col("b.status").isin(["CLOSED", "FROZEN"]))
    .select("src.*", F.col("b.status"), F.col("b.opened_date"), F.col("b.account_type"))
)

enriched = (
    joined
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "status",
        "opened_date",
        "account_type",
        F.min("amount").over(w3).alias("txn_time_month_min_amount"),
        F.max("amount").over(w4).alias("amount_max_90d"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

joined_transactions = (
    enriched
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.amount",
        "s.is_flagged",
        "s.status",
        "s.txn_time_month_min_amount",
        "s.seq_no",
        F.col("a.status").alias("account_status"),
        F.col("a.opened_date").alias("account_opened_date"),
        F.col("a.balance"),
    )
)

matched_keys = (
    joined_transactions
    .select("id", "account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "account_id")
    )
)

verified = (
    joined_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.status").alias("account_status_2"))
)

result = (
    with_dims
    .select("status", "balance", F.col("amount").alias("AMOUNT_VAL"))
    .unpivot(
        ids=["status"],
        values=["balance", "AMOUNT_VAL"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
