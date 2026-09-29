from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("account_type", "customer_id").orderBy("opened_date_month_end", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("account_type").orderBy(F.col("prev_balance").desc(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("prev_balance").desc_nulls_first())
w6 = Window.orderBy("prev_balance", "id")

filtered = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-03-28")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.first("status", ignorenulls=True).over(w).alias("first_known_status"),
        F.lag("balance", 1, 0).over(w_running).alias("prev_balance"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

with_windows = (
    with_last
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "opened_date_month_end",
        "prev_balance",
        "transaction_txn_time",
        F.ntile(5).over(w_hist).alias("prev_balance_bucket"),
        F.dense_rank().over(w5).alias("prev_balance_dense_rank"),
        F.ntile(5).over(w6).alias("prev_balance_bucket_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_balance") >= 67000, "HIGH").otherwise("LOW").alias("prev_balance_tier"),
        F.date_trunc("year", "transaction_txn_time").alias("year_start"),
    )
    .agg(
        F.percentile_approx("prev_balance", [0.25, 0.5, 0.75]).alias("prev_balance_quartiles"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.count_if(F.col("account_type") == "CREDIT").alias("credit_events"),
        F.max("prev_balance").alias("max_prev_balance"),
        ((F.unix_timestamp(F.max("transaction_txn_time")) - F.unix_timestamp(F.min("transaction_txn_time"))) / 60).alias("span_minutes"),
        F.round(F.avg("prev_balance"), 2).alias("avg_prev_balance"),
    )
    .orderBy(F.col("credit_events").desc())
)
