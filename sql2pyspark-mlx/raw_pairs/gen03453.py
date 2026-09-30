from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("amount")
w2 = Window.partitionBy("merchant").orderBy(F.col("amount_rank").asc_nulls_last())
w_recent = Window.orderBy(F.col("amount_rank").desc())

matched_keys = (
    df_transactions
    .select("account_id", "txn_type")
    .exceptAll(
        df_transactions
        .filter(F.col("txn_time") < "2024-05-01")
        .select("account_id", "txn_type")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.hour("txn_time").between(7, 10))
    .select(
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        F.rank().over(w_hist).alias("amount_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("txn_time") >= F.lit("2023-10-10 22:00:00").cast("timestamp"))
    .withColumn("txn_time_date", F.to_date("txn_time"))
)

scored = (
    base
    .select(
        "*",
        F.dense_rank().over(w2).alias("amount_rank_dense_rank"),
        F.cume_dist().over(w_recent).alias("amount_rank_cume_dist"),
    )
)

remaining_keys = (
    scored
    .select("txn_type", "merchant")
    .subtract(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("txn_type", "merchant")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("week", "txn_time").alias("week_start"))
    .agg(
        F.min("txn_time").alias("first_seen"),
        F.round(F.avg("amount_rank"), 2).alias("avg_amount_rank"),
        F.percentile_approx("amount_rank_cume_dist", [0.25, 0.5, 0.75]).alias("amount_rank_cume_dist_quartiles"),
        F.percentile_approx("amount_rank_cume_dist", [0.25, 0.5, 0.75]).alias("amount_rank_cume_dist_quartiles_2"),
    )
)
