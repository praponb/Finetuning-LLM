from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.orderBy("next_balance", "id")
w3 = Window.orderBy("prev_balance", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy(F.month("opened_date")).orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("event_seq", "id")
w6 = Window.orderBy("event_seq")
w7 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("event_seq").desc())
w9 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w11 = Window.partitionBy("transaction_txn_type").orderBy("prev_balance_avg_2_prior", "id")
w12 = Window.partitionBy("transaction_txn_type").orderBy("next_balance", "id")
w13 = Window.partitionBy("transaction_txn_type").orderBy(F.col("first_known_prev_balance_avg_2_prior").desc())
w14 = Window.partitionBy(F.date_trunc("week", "transaction_txn_time"), "transaction_txn_type").orderBy("first_known_prev_balance", "id")
w15 = Window.partitionBy("transaction_txn_type").orderBy("first_known_prev_balance_avg_2_prior", "id")
w16 = Window.orderBy("next_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_accounts
    .select(
        "id",
        "balance",
        "opened_date",
        F.lead("balance").over(w_ordered).alias("next_balance"),
        F.lag("balance").over(w_ordered).alias("prev_balance"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

scored_accounts = (
    scored
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.avg("prev_balance").over(w3).alias("prev_balance_avg_2_prior"),
        F.first("row_seq", ignorenulls=True).over(w4).alias("first_known_row_seq"),
    )
)

metrics = (
    scored_accounts
    .filter(F.col("prev_balance") >= 34000)
    .select(
        "id",
        "balance",
        "opened_date",
        "next_balance",
        "row_seq",
        "prev_balance_avg_2_prior",
        F.row_number().over(w5).alias("event_seq_2"),
        F.cume_dist().over(w6).alias("event_seq_cume_dist"),
        F.first("prev_balance_avg_2_prior", ignorenulls=True).over(w7).alias("first_known_prev_balance_avg_2_prior"),
        F.dense_rank().over(w8).alias("event_seq_dense_rank"),
        F.first("prev_balance", ignorenulls=True).over(w9).alias("first_known_prev_balance"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

scored_accounts_2 = (
    with_latest
    .select(
        "id",
        "balance",
        "opened_date",
        "next_balance",
        "first_known_prev_balance_avg_2_prior",
        "first_known_prev_balance",
        "transaction_txn_time",
        F.ntile(3).over(w11).alias("prev_balance_avg_2_prior_bucket"),
        F.row_number().over(w12).alias("seq_no"),
        F.dense_rank().over(w13).alias("first_known_prev_balance_avg_2_prior_dense_rank"),
        F.ntile(10).over(w14).alias("first_known_prev_balance_bucket"),
        F.row_number().over(w15).alias("row_seq_2"),
    )
)

enriched = (
    scored_accounts_2
    .select(
        "id",
        "first_known_prev_balance_avg_2_prior",
        "first_known_prev_balance",
        "seq_no",
        "first_known_prev_balance_bucket",
        F.min("first_known_prev_balance").over(w16).alias("running_min_first_known_prev_balance"),
        F.count("first_known_prev_balance").over(w16).alias("running_count_first_known_prev_balance"),
        F.sum("first_known_prev_balance").over(w16).alias("running_sum_first_known_prev_balance"),
        F.max("first_known_prev_balance").over(w16).alias("running_max_first_known_prev_balance"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isin(["Amazon", "Grab", "Shell"])), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "first_known_prev_balance_avg_2_prior",
        "first_known_prev_balance",
        "seq_no",
        "first_known_prev_balance_bucket",
        "running_min_first_known_prev_balance",
        "running_count_first_known_prev_balance",
        "running_sum_first_known_prev_balance",
        "running_max_first_known_prev_balance",
    )
    .orderBy(F.col("first_known_prev_balance").asc_nulls_first())
)
