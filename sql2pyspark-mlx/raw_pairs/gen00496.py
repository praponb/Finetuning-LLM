from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("balance", "id")
w4 = Window.orderBy("balance", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("merchant", "status").orderBy("balance_bucket", "id")
w7 = Window.partitionBy("status").orderBy("balance_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("txn_type").orderBy(F.col("balance").desc_nulls_first(), "id")

metrics = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.avg("balance").over(w).alias("balance_avg_5"),
        F.lead("balance", 2).over(w_running).alias("next_balance"),
        F.ntile(5).over(w3).alias("balance_bucket"),
        F.avg("balance").over(w4).alias("balance_avg_2_prior"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.status",
        "s.balance_avg_5",
        "s.next_balance",
        "s.balance_bucket",
        F.col("c.merchant"),
        F.col("c.txn_type"),
    )
)

enriched = (
    with_last
    .filter(F.col("balance") <= 33000)
    .select(
        "*",
        F.lead("next_balance").over(w6).alias("next_next_balance"),
        F.lag("balance").over(w6).alias("prev_balance"),
        F.lead("balance").over(w6).alias("next_balance_2"),
        F.row_number().over(w6).alias("row_seq"),
        F.last("next_balance", ignorenulls=True).over(w7).alias("last_known_next_balance"),
    )
)

with_last_accounts = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.txn_type",
        F.col("c.amount").alias("transaction_amount"),
    )
)

latest = (
    with_last_accounts
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select("id", "balance", "transaction_amount")
    .orderBy("id", F.col("transaction_amount").desc())
    .limit(10)
)
