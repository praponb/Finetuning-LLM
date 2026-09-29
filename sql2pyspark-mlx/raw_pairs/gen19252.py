from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("opened_date"), "account_type")
w2 = Window.partitionBy("customer_id")

matched_keys = (
    df_accounts
    .select("account_type")
    .intersectAll(
        df_accounts
        .filter(F.col("customer_id").isNotNull())
        .select("account_type")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "balance",
        "opened_date",
        "status",
        F.avg("balance").over(w).alias("opened_date_year_avg_balance"),
        F.sum("balance").over(w2).alias("customer_id_sum_balance"),
    )
)

base = (
    enriched
    .filter(F.col("opened_date_year_avg_balance").isNotNull() & (F.col("opened_date") >= F.lit("2023-04-03").cast("date")))
    .select(
        "*",
        F.floor("customer_id_sum_balance").alias("customer_id_sum_balance_whole"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

base_accounts = (
    base
    .filter(F.col("opened_date_year_avg_balance") == 58036)
    .withColumn("customer_id_sum_balance_per_customer_id_sum_balance_whole", (F.col("customer_id_sum_balance") / F.nullif("customer_id_sum_balance_whole", F.lit(0))))
    .withColumn("status_filled", F.coalesce("status", F.lit("OTHER")))
)

daily = (
    base_accounts
    .groupBy(F.date_trunc("month", "opened_date").alias("activity_month"), "status")
    .agg(
        F.percentile_approx("customer_id_sum_balance_per_customer_id_sum_balance_whole", 0.9).alias("customer_id_sum_balance_per_customer_id_sum_balance_whole_p90"),
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
        F.avg(F.when(F.col("status") == "FROZEN", F.col("customer_id_sum_balance_whole"))).alias("frozen_avg_customer_id_sum_balance_whole"),
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    daily
    .select(
        "activity_month",
        "status",
        "customer_id_sum_balance_per_customer_id_sum_balance_whole_p90",
        "balance_p95",
        "frozen_avg_customer_id_sum_balance_whole",
        "open_count",
    )
    .orderBy(F.col("balance_p95").asc_nulls_last())
)
