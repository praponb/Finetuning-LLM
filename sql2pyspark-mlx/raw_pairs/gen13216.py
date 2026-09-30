from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_last())
w_running = Window.partitionBy(F.year("opened_date"), "status").orderBy(F.col("balance").desc_nulls_first())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("transaction_merchant").orderBy("opened_date", "id")
w7 = Window.partitionBy("transaction_merchant").orderBy("next_balance_cume_dist", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy(F.year("opened_date")).orderBy(F.col("balance_cume_dist").desc(), "id")
w9 = Window.orderBy(F.col("prev_balance_cume_dist").desc())
w10 = Window.partitionBy(F.year("opened_date")).orderBy("next_balance_cume_dist", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("transaction_merchant").orderBy(F.col("next_balance_cume_dist").desc_nulls_last())
w_hist = Window.orderBy("prev_balance_cume_dist_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("transaction_merchant").orderBy("balance_cume_dist_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w15 = Window.orderBy("prev_balance_cume_dist_cume_dist", "id")
w16 = Window.orderBy(F.col("prev_balance_cume_dist").desc(), "id")
w17 = Window.orderBy("prev_balance_cume_dist", "id")
w18 = Window.orderBy("transaction_amount", "id").rowsBetween(-5, Window.currentRow)
w19 = Window.orderBy("next_balance_cume_dist_dense_rank", "id").rowsBetween(-5, -1)

enriched = (
    df_accounts
    .filter(F.year("opened_date") == 2024)
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.cume_dist().over(w_recent).alias("balance_cume_dist"),
        F.rank().over(w_recent).alias("balance_rank"),
        F.sum("balance").over(w2).alias("running_balance"),
        F.rank().over(w3).alias("balance_rank_2"),
        F.percent_rank().over(w_running).alias("balance_percent_rank"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.balance_cume_dist",
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

with_windows = (
    latest_match
    .select(
        "*",
        F.lag("balance_cume_dist").over(w_part).alias("prev_balance_cume_dist"),
        F.lead("balance_cume_dist").over(w_part).alias("next_balance_cume_dist"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

with_windows_accounts = (
    with_windows
    .withColumn("next_balance_cume_dist_avg_5", F.avg("next_balance_cume_dist").over(w7))
    .withColumn("balance_cume_dist_bucket", F.ntile(4).over(w8))
    .withColumn("prev_balance_cume_dist_cume_dist", F.cume_dist().over(w9))
    .withColumn("event_seq_avg_6_prior", F.avg("event_seq").over(w10))
)

enriched_accounts = (
    with_windows_accounts
    .select(
        "id",
        "opened_date",
        "balance_cume_dist",
        "transaction_merchant",
        "prev_balance_cume_dist",
        "next_balance_cume_dist",
        "event_seq",
        "balance_cume_dist_bucket",
        "prev_balance_cume_dist_cume_dist",
        "event_seq_avg_6_prior",
        F.dense_rank().over(w11).alias("next_balance_cume_dist_dense_rank"),
        F.first("balance_cume_dist_bucket", ignorenulls=True).over(w_hist).alias("first_known_balance_cume_dist_bucket"),
        F.first("prev_balance_cume_dist", ignorenulls=True).over(w13).alias("first_known_prev_balance_cume_dist"),
    )
)

with_latest = (
    enriched_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w14))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_balance_cume_dist",
        "s.next_balance_cume_dist",
        "s.event_seq",
        "s.balance_cume_dist_bucket",
        "s.prev_balance_cume_dist_cume_dist",
        "s.next_balance_cume_dist_dense_rank",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_time"),
    )
)

with_windows_accounts_2 = (
    with_latest
    .select(
        "id",
        "prev_balance_cume_dist",
        "event_seq",
        "next_balance_cume_dist_dense_rank",
        "txn_time",
        F.row_number().over(w15).alias("seq_no"),
        F.ntile(5).over(w16).alias("prev_balance_cume_dist_bucket"),
        F.lag("next_balance_cume_dist_dense_rank", 1).over(w17).alias("prev_next_balance_cume_dist_dense_rank"),
        F.avg("balance_cume_dist_bucket").over(w18).alias("balance_cume_dist_bucket_avg_5"),
        F.avg("prev_balance_cume_dist").over(w19).alias("prev_balance_cume_dist_avg_5_prior"),
    )
)

result = (
    with_windows_accounts_2
    .select(
        "id",
        "prev_balance_cume_dist",
        "txn_time",
        "prev_balance_cume_dist_bucket",
        "prev_next_balance_cume_dist_dense_rank",
        "balance_cume_dist_bucket_avg_5",
    )
    .orderBy(F.col("prev_balance_cume_dist").desc_nulls_last())
)
