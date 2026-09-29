from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id")
w_hist = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("first_known_balance", "id")
w_recent = Window.partitionBy("status", "customer_id").orderBy("first_known_balance", "id")
w7 = Window.partitionBy("status").orderBy("row_seq", "id")
w_part = Window.partitionBy("status").orderBy("prev_status_avg_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("next_status_avg_balance").asc_nulls_first())
w10 = Window.partitionBy("prev_first_known_balance_tier").orderBy(F.col("min_prev_status_avg_balance_2").desc())

with_windows = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "status",
        F.avg("balance").over(w).alias("status_avg_balance"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("balance", ignorenulls=True).over(w_hist).alias("first_known_balance"),
        F.first("status", ignorenulls=True).over(w4).alias("first_known_status"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "status",
        "status_avg_balance",
        "row_seq",
        "first_known_balance",
    )
)

with_windows_accounts = (
    ranked
    .filter(F.col("customer_id").isin([15, 25, 40]))
    .select(
        "id",
        "status",
        "row_seq",
        F.lag("status_avg_balance").over(w_recent).alias("prev_status_avg_balance"),
        F.lead("status_avg_balance").over(w_recent).alias("next_status_avg_balance"),
        F.lag("first_known_balance").over(w_recent).alias("prev_first_known_balance"),
        F.lead("first_known_balance").over(w_recent).alias("next_first_known_balance"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.lag("status_avg_balance", 2).over(w7).alias("prev_status_avg_balance_2"),
    )
)

kept = (
    with_windows_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([4, 14, 34])), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant") == "Lazada"), "left_semi")
)

scored = (
    not_flagged
    .select(
        "id",
        "row_seq",
        "prev_status_avg_balance",
        "prev_first_known_balance",
        "next_first_known_balance",
        "event_seq",
        "prev_status_avg_balance_2",
        F.first("prev_first_known_balance", ignorenulls=True).over(w_part).alias("first_known_prev_first_known_balance"),
        F.cume_dist().over(w_grp).alias("next_status_avg_balance_cume_dist"),
    )
)

totals = (
    scored
    .groupBy(
        F.when(F.col("prev_first_known_balance") >= 62000, "HIGH").otherwise("LOW").alias("prev_first_known_balance_tier"),
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.min("prev_status_avg_balance_2").alias("min_prev_status_avg_balance_2"),
        F.percentile_approx("prev_status_avg_balance", [0.25, 0.5, 0.75]).alias("prev_status_avg_balance_quartiles"),
        F.min("prev_first_known_balance").alias("min_prev_first_known_balance"),
        F.percentile_approx("next_first_known_balance", [0.25, 0.5, 0.75]).alias("next_first_known_balance_quartiles"),
        F.count("*").alias("cnt"),
    )
    .withColumn("min_prev_status_avg_balance_2_dense_rank", F.dense_rank().over(w10))
)

result = (
    totals
    .select(
        "prev_first_known_balance_tier",
        "row_seq_tier",
        "prev_status_avg_balance_quartiles",
        "min_prev_first_known_balance",
        "min_prev_status_avg_balance_2_dense_rank",
    )
    .orderBy(F.col("row_seq_tier").desc())
)
