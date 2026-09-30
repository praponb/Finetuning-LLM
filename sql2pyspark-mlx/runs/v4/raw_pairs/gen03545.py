from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("prev_balance").desc(), "id")
w5 = Window.partitionBy("account_type")
w6 = Window.orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("account_type", "status").orderBy("balance_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("account_type_max_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("account_type").orderBy("account_type_max_balance", "id")
w_part_2 = Window.partitionBy("status", "account_type").orderBy("account_type_max_balance", "id")
w11 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w12 = Window.partitionBy("account_type").orderBy("balance_bucket_avg_5", "id")
w13 = Window.partitionBy("status", "account_type").orderBy(F.col("last_known_balance_bucket").asc_nulls_first())
w14 = Window.partitionBy("status").orderBy(F.col("account_type_max_balance_bucket").desc_nulls_first())
w15 = Window.partitionBy("status", "transaction_merchant").orderBy("last_known_balance_bucket", "id")

cleaned = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.trim("account_type").alias("clean_account_type"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.ntile(5).over(w).alias("balance_bucket"),
        F.lag("balance", 1).over(w2).alias("prev_balance"),
        F.last("status", ignorenulls=True).over(w3).alias("last_known_status"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.max("balance").over(w5).alias("account_type_max_balance"),
        F.avg("balance_bucket").over(w6).alias("balance_bucket_avg_5"),
        F.last("balance_bucket", ignorenulls=True).over(w7).alias("last_known_balance_bucket"),
    )
)

metrics_accounts = (
    metrics
    .withColumn("cumulative_account_type_max_balance", F.sum("account_type_max_balance").over(w_running))
    .withColumn("prev_balance_2", F.lag("balance", 1).over(w_part))
    .withColumn("account_type_max_balance_bucket", F.ntile(5).over(w_part_2))
)

kept = (
    metrics_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") == "FEE"), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w11))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

result = (
    with_last
    .withColumn("prev_prev_balance", F.lag("prev_balance", 2, 0).over(w12))
    .withColumn("last_known_balance_bucket_percent_rank", F.percent_rank().over(w13))
    .withColumn("account_type_max_balance_bucket_cume_dist", F.cume_dist().over(w14))
    .withColumn("next_transaction_amount", F.lead("transaction_amount", 2).over(w15))
)
