from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("YEAR", "txn_time")).orderBy("amount_whole", "id")
w3 = Window.partitionBy("merchant").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.year("opened_date"), "account_type").orderBy("running_count_amount_minus_amount_whole_bucket", "id")
w_recent = Window.orderBy("running_count_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w7 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("merchant", "account_type").orderBy("running_count_amount_minus_amount_whole_bucket_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("account_id").isin([6, 29, 38]))
    .select("*", F.floor("amount").alias("amount_whole"))
)

with_windows = (
    base
    .withColumn("running_avg_amount", F.avg("amount").over(w))
    .withColumn("running_count_amount", F.count("amount").over(w))
    .withColumn("running_sum_amount", F.sum("amount").over(w))
    .withColumn("amount_whole_bucket", F.ntile(3).over(w2))
)

enriched = (
    with_windows
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .filter(F.month(F.col("a.opened_date")).isin([4, 8, 9]))
    .select("e.*", F.col("a.account_type"), F.col("a.status"), F.col("a.opened_date"))
)

valid = (
    enriched
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2023-08-05 18:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        "running_avg_amount",
        "running_count_amount",
        "running_sum_amount",
        "amount_whole_bucket",
        "account_type",
        "opened_date",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

filtered = (
    valid
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-04-16")))
    .select(
        "*",
        (F.col("running_count_amount") - F.col("amount_whole_bucket")).alias("running_count_amount_minus_amount_whole_bucket"),
    )
)

with_windows_transactions = (
    filtered
    .withColumn("running_amount_whole_bucket", F.sum("amount_whole_bucket").over(w3))
    .withColumn("running_count_amount_minus_amount_whole_bucket_bucket", F.ntile(10).over(w_grp))
    .withColumn("cumulative_running_count_amount", F.sum("running_count_amount").over(w_recent))
)

flagged = (
    with_windows_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w6) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w7))

result = (
    sessionized
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        "running_avg_amount",
        "running_sum_amount",
        "opened_date",
        "resolved_status",
        "running_count_amount_minus_amount_whole_bucket",
        "session_no",
        F.avg("running_count_amount").over(w8).alias("running_avg_running_count_amount"),
        F.max("running_count_amount").over(w8).alias("running_max_running_count_amount"),
        F.sum("running_count_amount").over(w8).alias("running_sum_running_count_amount"),
    )
)
