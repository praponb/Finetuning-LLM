from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_status")
w2 = Window.partitionBy(F.year("txn_time_date"), "account_status").orderBy("opened_date", "id").rowsBetween(-3, -1)
w3 = Window.orderBy(F.col("account_balance").asc_nulls_last())
w4 = Window.partitionBy(F.month("opened_date")).orderBy("opened_date", "id")
w5 = Window.partitionBy("status").orderBy("next_account_balance", "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-05-25"))) & F.col("merchant").isNotNull())
    .withColumn("txn_time_period", F.date_trunc("year", "txn_time"))
    .withColumn("txn_time_date", F.to_date("txn_time"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("DIRECT")))
)

enriched = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type") == "LOAN")
    .select(
        "e.id",
        "e.account_id",
        "e.txn_time",
        "e.txn_time_date",
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.account_type"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_time_date",
        "a.status",
        F.col("p2.balance").alias("account_balance"),
        F.col("p2.status").alias("account_status"),
        F.col("p2.opened_date"),
    )
)

scored = (
    with_dims
    .select(
        "*",
        F.count("account_balance").over(w).alias("account_status_count_account_balance"),
        F.avg("account_balance").over(w2).alias("account_balance_avg_3_prior"),
        F.percent_rank().over(w3).alias("account_balance_percent_rank"),
        F.lead("account_balance", 1).over(w4).alias("next_account_balance"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("week", "txn_time_date").alias("week_start"))
    .agg(
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("account_balance", 0.95).alias("account_balance_p95"),
        F.round(F.avg("account_balance"), 2).alias("avg_account_balance"),
    )
    .orderBy(F.col("week_start").asc_nulls_first())
)
