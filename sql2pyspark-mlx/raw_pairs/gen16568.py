from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w_part = Window.orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w5 = Window.orderBy("balance", "id")
w6 = Window.partitionBy("txn_type")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.account_type", "s.balance", F.col("c.txn_type"))
)

with_windows = (
    with_last
    .select(
        "id",
        "account_type",
        "balance",
        F.ntile(5).over(w2).alias("balance_bucket"),
        F.row_number().over(w_part).alias("row_seq"),
        F.rank().over(w4).alias("balance_rank"),
        F.row_number().over(w5).alias("event_seq"),
        F.sum("balance").over(w6).alias("txn_type_sum_balance"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.account_id").isin([24, 29, 32, 35])), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

cleaned = (
    with_latest
    .filter(F.col("event_seq") != 6)
    .select(
        "*",
        F.greatest("balance_bucket", "txn_type_sum_balance").alias("greatest_balance_bucket_txn_type_sum_balance"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "account_type",
        "balance_bucket",
        "balance_rank",
        "txn_type_sum_balance",
        "greatest_balance_bucket_txn_type_sum_balance",
    )
)
