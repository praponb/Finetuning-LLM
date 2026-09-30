from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_status", "account_id").orderBy("balance", "id")
w2 = Window.partitionBy(F.year("opened_date"), "account_type").orderBy("opened_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy(F.year("opened_date"), "status").orderBy(F.col("next_balance").asc_nulls_last())
w_running = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-06-01 14:00:00").cast("timestamp")) & (F.col("amount") > 2800))
    .select(
        "*",
        F.when(F.col("merchant") == "Starbucks", 1).otherwise(0).alias("is_starbucks"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select("a.id", "a.account_id", "a.merchant", F.col("d.account_type"))
)

joined = (
    enriched
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.id", "e.account_id", "e.merchant", "e.account_type", F.col("d.status"))
)

joined_transactions = (
    joined
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.opened_date"), F.col("d.status").alias("account_status"))
)

enriched_transactions = (
    joined_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .filter(F.month(F.col("p.opened_date")).isin([4, 8, 11]))
    .select("s.*", F.col("p.balance"))
)

with_windows = (
    enriched_transactions
    .select(
        "id",
        "account_type",
        "status",
        "opened_date",
        "balance",
        F.lead("balance").over(w_part).alias("next_balance"),
        F.lag("balance").over(w_part).alias("prev_balance"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

with_windows_transactions = (
    with_windows
    .select(
        "id",
        "status",
        "opened_date",
        F.avg("balance").over(w2).alias("balance_avg_2_prior"),
        F.rank().over(w3).alias("next_balance_rank"),
        F.sum("balance").over(w_running).alias("cumulative_balance"),
    )
)

result = (
    with_windows_transactions
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.round(F.avg("balance_avg_2_prior"), 2).alias("avg_balance_avg_2_prior"),
        F.min("balance_avg_2_prior").alias("min_balance_avg_2_prior"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("next_balance_rank", 0.95).alias("next_balance_rank_p95"),
    )
)
