from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy(F.col("transaction_amount").desc_nulls_first())
w_running = Window.partitionBy("status").orderBy(F.col("transaction_txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("month", "transaction_txn_time"), "status").orderBy(F.col("weeks_since_opened_date").desc())
w5 = Window.partitionBy("status").orderBy(F.col("transaction_amount").asc_nulls_first())
w6 = Window.orderBy("transaction_amount")

base = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select(
        "*",
        (F.datediff(F.current_date(), "opened_date") / 7).alias("weeks_since_opened_date"),
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.weeks_since_opened_date",
        "s.account_type_status_key",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

remaining_keys = (
    with_latest
    .select("status", "id")
    .exceptAll(
        df_accounts
        .filter(F.col("status") == "CLOSED")
        .select("status", "id")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = kept_rows.withColumn("transaction_amount_rank", F.rank().over(w2))

scored = (
    with_windows
    .filter(F.col("weeks_since_opened_date") >= 36)
    .select(
        "weeks_since_opened_date",
        "transaction_amount",
        "transaction_amount_rank",
        F.count("transaction_amount_rank").over(w_running).alias("transaction_amount_rank_count_90d"),
        F.cume_dist().over(w4).alias("weeks_since_opened_date_cume_dist"),
        F.avg("transaction_amount_rank").over(w_running).alias("transaction_amount_rank_avg_90d"),
        F.dense_rank().over(w5).alias("transaction_amount_dense_rank"),
    )
)

result = (
    scored
    .select(
        "weeks_since_opened_date",
        "transaction_amount",
        "transaction_amount_rank",
        "weeks_since_opened_date_cume_dist",
        "transaction_amount_rank_avg_90d",
        F.rank().over(w6).alias("transaction_amount_rank_2"),
    )
)
