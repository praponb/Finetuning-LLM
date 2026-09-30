from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.to_date("txn_time"), "account_type")
w3 = Window.partitionBy("status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_recent = Window.partitionBy(F.to_date("txn_time"), "account_type").orderBy("opened_date", "id")
w5 = Window.partitionBy("merchant", "status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("merchant", "account_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("next_balance_max_7d", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy(F.month("opened_date"), "status").orderBy(F.col("next_balance").desc(), "id")

base = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("opened_date") > F.lit("2023-09-18").cast("date")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.status_filled",
        F.col("c.txn_time"),
        F.col("c.merchant"),
    )
)

enriched = (
    latest_match
    .select(
        "*",
        F.sum("balance").over(w2).alias("txn_time_to_date_sum_balance"),
        F.count("balance").over(w3).alias("balance_count_90d"),
        F.lead("balance", 2).over(w_recent).alias("next_balance"),
    )
)

enriched_accounts = (
    enriched
    .select(
        "id",
        "opened_date",
        "status",
        "txn_time",
        "txn_time_to_date_sum_balance",
        "next_balance",
        F.count("next_balance").over(w5).alias("next_balance_count_7d"),
        F.max("next_balance").over(w6).alias("next_balance_max_7d"),
    )
)

metrics = (
    enriched_accounts
    .withColumn("last7_sum_next_balance_count_7d", F.sum("next_balance_count_7d").over(w7))
    .withColumn("last7_avg_next_balance_count_7d", F.avg("next_balance_count_7d").over(w7))
    .withColumn("last7_count_next_balance_count_7d", F.count("next_balance_count_7d").over(w7))
    .withColumn("event_seq", F.row_number().over(w8))
)

summary = (
    metrics
    .groupBy(F.date_trunc("WEEK", "txn_time").alias("week_start"))
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.percentile_approx("txn_time_to_date_sum_balance", 0.5).alias("txn_time_to_date_sum_balance_p50"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("last7_avg_next_balance_count_7d")).otherwise(0)).alias("open_last7_avg_next_balance_count_7d"),
        F.percentile_approx("next_balance_count_7d", 0.5).alias("next_balance_count_7d_p50"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
    )
)

result = (
    summary
    .select(
        "week_start",
        "txn_time_to_date_sum_balance_p50",
        "span_minutes",
        "closed_rows",
    )
    .orderBy("week_start")
)
