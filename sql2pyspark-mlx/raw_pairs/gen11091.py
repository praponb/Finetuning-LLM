from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("account_type", "status").orderBy("transaction_txn_time", "id")
w_hist = Window.orderBy("prev_prev_balance", "id").rowsBetween(-2, -1)
w5 = Window.orderBy("prev_balance_2", "id")
w6 = Window.orderBy("prev_prev_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("prev_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("prev_prev_balance", "id")

with_windows = (
    df_accounts
    .select(
        "*",
        F.lag("balance").over(w).alias("prev_balance"),
        F.lead("balance").over(w).alias("next_balance"),
        F.row_number().over(w).alias("seq_no"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([5, 17, 19, 34])), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.prev_balance",
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

with_windows_accounts = (
    with_last
    .select(
        "id",
        "prev_balance",
        F.lag("balance").over(w_ordered).alias("prev_balance_2"),
        F.lag("prev_balance").over(w_ordered).alias("prev_prev_balance"),
        F.lead("prev_balance").over(w_ordered).alias("next_prev_balance"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

not_flagged_accounts = (
    with_windows_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.is_flagged")), "left_anti")
)

eligible = (
    not_flagged_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant") == "Shell"), "left_anti")
)

enriched = (
    eligible
    .filter(F.col("row_seq") <= 4)
    .withColumn("row_seq_avg_2_prior", F.avg("row_seq").over(w_hist))
    .withColumn("next_row_seq", F.lead("row_seq", 1).over(w5))
    .withColumn("first_known_prev_balance_2", F.first("prev_balance_2", ignorenulls=True).over(w6))
    .withColumn("cumulative_prev_balance", F.sum("prev_balance").over(w_grp))
    .withColumn("next_prev_balance_2", F.lead("prev_balance_2", 1, 0).over(w8))
)

result = (
    enriched
    .rollup(
        F.when(F.col("prev_balance") >= 69000, "LARGE").otherwise("SMALL").alias("prev_balance_tier"),
        F.when(F.col("cumulative_prev_balance") >= 317260, "HIGH").otherwise("LOW").alias("cumulative_prev_balance_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum("next_row_seq").alias("total_next_row_seq"),
        F.round(F.avg("next_prev_balance"), 2).alias("avg_next_prev_balance"),
    )
)
