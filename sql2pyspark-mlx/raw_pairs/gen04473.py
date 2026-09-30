from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time_day", "id")
w3 = Window.partitionBy("status", "account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("txn_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Lazada"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.dayofmonth("txn_time").alias("txn_time_day"),
        F.upper("merchant").alias("merchant_upper"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.balance"), F.col("b.status"), F.col("b.account_type"))
)

with_windows = (
    joined
    .select(
        "id",
        "account_id",
        "txn_type",
        "is_flagged",
        "txn_time_day",
        "merchant_upper",
        "balance",
        "status",
        "account_type",
        F.row_number().over(w).alias("event_seq"),
        F.lead("balance", 1, 0).over(w2).alias("next_balance"),
    )
)

metrics = (
    with_windows
    .filter(F.col("account_id").isin([24, 34]))
    .select(
        "*",
        F.sum("event_seq").over(w3).alias("running_sum_event_seq"),
        F.max("event_seq").over(w3).alias("running_max_event_seq"),
        F.avg("event_seq").over(w3).alias("running_avg_event_seq"),
        F.sum("next_balance").over(w4).alias("cumulative_next_balance"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.is_flagged",
        "a.txn_time_day",
        "a.merchant_upper",
        "a.account_type",
        "a.event_seq",
        "a.next_balance",
        "a.running_sum_event_seq",
        "a.running_max_event_seq",
        "a.running_avg_event_seq",
        F.col("d.balance").alias("account_balance"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.is_flagged",
        "src.txn_time_day",
        "src.account_type",
        "src.next_balance",
        "src.running_max_event_seq",
        "src.running_avg_event_seq",
        "src.account_balance",
        F.col("b.opened_date"),
    )
)

result = (
    enriched
    .groupBy("txn_type", "account_id")
    .pivot("account_type", ["CREDIT", "CHECKING"])
    .agg(F.avg("running_avg_event_seq").alias("amount"))
)
