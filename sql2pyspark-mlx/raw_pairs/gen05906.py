from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy(F.col("txn_time").desc())
w2 = Window.orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy(F.year("txn_time"), "txn_type").orderBy("txn_time", "id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w5 = Window.orderBy("txn_time", "id")
w6 = Window.partitionBy("account_type")
w_running = Window.orderBy(F.col("account_type_count_balance").desc_nulls_first())
w8 = Window.orderBy(F.col("balance").asc_nulls_first())
w9 = Window.partitionBy(F.date_trunc("month", "opened_date")).orderBy(F.col("account_type_count_balance").asc_nulls_first())
w_grp = Window.orderBy(F.col("row_seq_2").desc_nulls_first())
w_hist = Window.orderBy(F.col("account_type_max_row_seq_whole").desc())

latest = (
    df_transactions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_id", "txn_type", "txn_time")
)

enriched = (
    latest
    .filter(F.col("account_id").isin([9, 30, 37, 38]))
    .select(
        "id",
        "account_id",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

matched_keys = (
    enriched
    .select("id", "account_id")
    .exceptAll(
        df_transactions
        .filter(F.hour("txn_time").between(8, 13))
        .select("id", "account_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("event_seq_2").between(5, 7))
    .select(
        "id",
        "account_id",
        "event_seq",
        "row_seq",
        "row_seq_2",
        "event_seq_2",
        F.floor("row_seq").alias("row_seq_whole"),
        F.when(F.col("row_seq_2") >= 8, "High").when(F.col("row_seq_2") >= 2, "Medium").otherwise("Low").alias("row_seq_2_band"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.event_seq",
        "src.row_seq_2",
        "src.event_seq_2",
        "src.row_seq_whole",
        "src.row_seq_2_band",
        F.col("p.opened_date"),
        F.col("p.balance"),
        F.col("p.account_type"),
    )
)

metrics = (
    with_dims
    .select(
        "event_seq",
        "row_seq_2",
        "row_seq_2_band",
        "opened_date",
        "balance",
        F.max("row_seq_whole").over(w6).alias("account_type_max_row_seq_whole"),
        F.count("balance").over(w6).alias("account_type_count_balance"),
    )
)

with_windows = (
    metrics
    .select(
        "balance",
        "account_type_max_row_seq_whole",
        "account_type_count_balance",
        F.percent_rank().over(w_running).alias("account_type_count_balance_percent_rank"),
        F.cume_dist().over(w8).alias("balance_cume_dist"),
        F.cume_dist().over(w9).alias("account_type_count_balance_cume_dist"),
        F.dense_rank().over(w_grp).alias("row_seq_2_dense_rank"),
        F.cume_dist().over(w_hist).alias("account_type_max_row_seq_whole_cume_dist"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("balance_cume_dist") >= 0.74, "LARGE").otherwise("SMALL").alias("balance_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("row_seq_2_dense_rank", [0.25, 0.5, 0.75]).alias("row_seq_2_dense_rank_quartiles"),
        F.percentile_approx("account_type_max_row_seq_whole", 0.5).alias("account_type_max_row_seq_whole_p50"),
        F.percentile_approx("balance_cume_dist", 0.95).alias("balance_cume_dist_p95"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy(F.col("balance_cume_dist_tier").desc())
)
