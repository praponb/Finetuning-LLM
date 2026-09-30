from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.orderBy("next_balance", "id")
w3 = Window.orderBy(F.col("is_flagged_flag").desc_nulls_last())
w4 = Window.orderBy(F.col("balance").desc_nulls_first())
w5 = Window.orderBy("row_seq_2", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.orderBy("prev_is_flagged_flag", "id")
w7 = Window.orderBy("prev_balance_2", "id")

cleaned = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Lazada"))) & (F.col("txn_time") > F.lit("2023-09-02 00:00:00").cast("timestamp")))
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat("merchant", F.lit("/"), "txn_type").alias("merchant_txn_type"),
        F.trim("merchant").alias("clean_merchant"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.balance"))
)

filtered = (
    joined
    .filter((~F.col("merchant").eqNullSafe(F.lit("Amazon"))) & (F.col("txn_time") >= "2023-05-01"))
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "merchant_txn_type",
        "clean_merchant",
        "balance",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

enriched = (
    filtered
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "is_flagged",
        "balance",
        "is_flagged_flag",
        F.lag("balance").over(w).alias("prev_balance"),
        F.lead("balance").over(w).alias("next_balance"),
        F.lead("is_flagged_flag").over(w).alias("next_is_flagged_flag"),
        F.row_number().over(w).alias("row_seq"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "is_flagged",
        "balance",
        "next_is_flagged_flag",
        F.lag("is_flagged_flag").over(w2).alias("prev_is_flagged_flag"),
        F.lag("balance").over(w2).alias("prev_balance_2"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.dense_rank().over(w3).alias("is_flagged_flag_dense_rank"),
    )
)

metrics_transactions = (
    metrics
    .select(
        "id",
        "is_flagged",
        "balance",
        "next_is_flagged_flag",
        "prev_balance_2",
        F.dense_rank().over(w4).alias("balance_dense_rank"),
        F.avg("next_is_flagged_flag").over(w5).alias("next_is_flagged_flag_avg_2"),
        F.lead("prev_balance_2", 1, 0).over(w_part).alias("next_prev_balance_2"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

cleaned_transactions = (
    metrics_transactions
    .filter(F.col("seq_no") > 7)
    .select(
        "id",
        "is_flagged",
        "balance",
        "next_is_flagged_flag",
        "balance_dense_rank",
        "next_is_flagged_flag_avg_2",
        F.ceil("seq_no").alias("seq_no_whole"),
        F.col("balance").cast("int").alias("balance_int"),
    )
)

result = (
    cleaned_transactions
    .groupBy(
        F.when(F.col("balance_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("balance_dense_rank_tier"),
        F.when(F.col("next_is_flagged_flag_avg_2") >= 64, "HIGH").otherwise("LOW").alias("next_is_flagged_flag_avg_2_tier"),
    )
    .agg(
        F.percentile_approx("balance_dense_rank", 0.5).alias("balance_dense_rank_p50"),
        F.round(F.avg("next_is_flagged_flag"), 2).alias("avg_next_is_flagged_flag"),
        F.min("balance_int").alias("min_balance_int"),
        F.percentile_approx("seq_no_whole", [0.25, 0.5, 0.75]).alias("seq_no_whole_quartiles"),
        F.avg("next_is_flagged_flag").alias("avg_next_is_flagged_flag_2"),
        F.min("next_is_flagged_flag_avg_2").alias("min_next_is_flagged_flag_avg_2"),
    )
    .filter(F.col("avg_next_is_flagged_flag_2") > 0)
)
