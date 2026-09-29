from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_type").orderBy("balance", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("merchant")
w4 = Window.partitionBy("account_type").orderBy(F.col("amount").desc(), "id")
w5 = Window.orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("account_account_type").orderBy("account_opened_date", "id")
w7 = Window.partitionBy("status", "account_account_type")
w8 = Window.partitionBy("account_status").orderBy("balance", "id")
w9 = Window.orderBy("merchant_stddev_samp_balance", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("account_id").orderBy("amount_avg_6", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

joined = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.amount",
        "s.merchant",
        "s.is_flagged",
        F.col("b.account_type"),
        F.col("b.status"),
        F.col("b.balance"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_2_prior"),
        F.avg("amount").over(w2).alias("amount_avg_5"),
        F.stddev_samp("balance").over(w3).alias("merchant_stddev_samp_balance"),
        F.ntile(5).over(w4).alias("amount_bucket"),
        F.avg("amount").over(w5).alias("amount_avg_6"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select(
        "e.*",
        F.col("a.opened_date"),
        F.col("a.account_type").alias("account_account_type"),
        F.col("a.status").alias("account_status"),
    )
)

joined_transactions = (
    enriched
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.opened_date").between("2025-10-01", "2025-10-26"))
    .select(
        "a.*",
        F.col("p2.balance").alias("account_balance"),
        F.col("p2.account_type").alias("account_account_type_2"),
        F.col("p2.opened_date").alias("account_opened_date"),
    )
)

with_windows_transactions = (
    joined_transactions
    .select(
        "id",
        "account_id",
        "is_flagged",
        "account_type",
        "status",
        "amount_avg_2_prior",
        "amount_avg_5",
        "opened_date",
        "account_balance",
        "account_opened_date",
        F.row_number().over(w6).alias("seq_no"),
        F.avg("balance").over(w7).alias("status_avg_balance"),
        F.row_number().over(w8).alias("event_seq"),
        F.avg("merchant_stddev_samp_balance").over(w9).alias("merchant_stddev_samp_balance_avg_3"),
        F.ntile(4).over(w10).alias("amount_avg_6_bucket"),
    )
)

result = (
    with_windows_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "is_flagged",
        "status",
        "amount_avg_2_prior",
        "amount_avg_5",
        "opened_date",
        "account_balance",
        "seq_no",
        F.col("avg_transaction_amount"),
    )
)
