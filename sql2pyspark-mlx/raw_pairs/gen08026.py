from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.orderBy("transaction_amount", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_hist = Window.orderBy("amount", "id")
w6 = Window.partitionBy(F.to_date("txn_time")).orderBy(F.col("balance").desc(), "id")
w7 = Window.orderBy("txn_time", "id")
w_running = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_accounts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.opened_date",
        "s.row_num",
        F.col("c.txn_time"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_semi")
)

scored = (
    eligible
    .select(
        "id",
        "balance",
        "opened_date",
        "row_num",
        F.lead("balance").over(w3).alias("next_balance"),
        F.lag("transaction_amount").over(w3).alias("prev_transaction_amount"),
        F.lead("transaction_amount").over(w3).alias("next_transaction_amount"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.balance", "s.row_num", F.col("c.txn_time"), F.col("c.amount"))
)

eligible_accounts = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.txn_time")), "left_anti")
)

metrics = (
    eligible_accounts
    .select(
        "*",
        F.ntile(10).over(w_hist).alias("amount_bucket"),
        F.ntile(3).over(w6).alias("balance_bucket"),
        F.lag("balance", 1, 0).over(w7).alias("prev_balance"),
    )
)

result = (
    metrics
    .select(
        "*",
        F.max("balance").over(w_running).alias("running_max_balance"),
        F.min("balance").over(w_running).alias("running_min_balance"),
        F.count("balance").over(w_running).alias("running_count_balance"),
    )
)
