from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id")
w3 = Window.partitionBy("account_type")
w4 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("balance").desc())
w6 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w8 = Window.partitionBy("status")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("status").orderBy("status_stddev_samp_account_type_max_balance", "id").rowsBetween(-3, Window.currentRow)
w11 = Window.partitionBy(F.date_trunc("MONTH", "transaction_txn_time"), "status").orderBy("last4_sum_seq_no", "id")
w12 = Window.orderBy("balance", "id")
w13 = Window.orderBy("amount", "id")

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.lower("account_type").alias("account_type_lower"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    ranked
    .select(
        "id",
        "balance",
        "status",
        F.lead("balance", 1).over(w2).alias("next_balance"),
        F.max("balance").over(w3).alias("account_type_max_balance"),
        F.sum("balance").over(w4).alias("running_balance"),
        F.dense_rank().over(w5).alias("balance_dense_rank"),
    )
)

enriched = (
    scored
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("status_stddev_samp_account_type_max_balance", F.stddev_samp("account_type_max_balance").over(w8))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"), F.col("c.amount"))
)

with_windows = (
    with_latest
    .withColumn("last4_min_seq_no", F.min("seq_no").over(w_ordered))
    .withColumn("last4_avg_seq_no", F.avg("seq_no").over(w_ordered))
    .withColumn("last4_max_seq_no", F.max("seq_no").over(w_ordered))
    .withColumn("last4_sum_seq_no", F.sum("seq_no").over(w_ordered))
)

enriched_accounts = (
    with_windows
    .select(
        "id",
        "status",
        "next_balance",
        "status_stddev_samp_account_type_max_balance",
        "transaction_txn_time",
        "amount",
        "last4_avg_seq_no",
        "last4_max_seq_no",
        F.lead("next_balance", 1).over(w11).alias("next_next_balance"),
        F.ntile(3).over(w12).alias("balance_bucket"),
        F.lag("account_type_max_balance", 1).over(w13).alias("prev_account_type_max_balance"),
    )
)

grouped = (
    enriched_accounts
    .groupBy(F.date_trunc("YEAR", "transaction_txn_time").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("transaction_txn_time")) - F.unix_timestamp(F.min("transaction_txn_time"))) / 60).alias("duration_min"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("distinct_status") < 1000)
)

result = (
    grouped
    .select("year_start", "duration_min")
    .orderBy(F.col("year_start").desc_nulls_first())
)
