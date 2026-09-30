from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("opened_date"), "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("balance").desc())
w6 = Window.partitionBy("account_type", "status")
w7 = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "account_type").orderBy("prev_balance", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("account_type", "status").orderBy(F.col("first_known_balance").desc(), "id")

base = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-10-04")))
    .select(
        "*",
        F.when(F.col("balance") >= 65000, "High").when(F.col("balance") >= 32000, "Medium").otherwise("Low").alias("balance_band"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_band",
        F.last("status", ignorenulls=True).over(w).alias("last_known_status"),
        F.lag("balance", 2).over(w2).alias("prev_balance"),
        F.first("balance", ignorenulls=True).over(w3).alias("first_known_balance"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("status", ignorenulls=True).over(w4).alias("last_known_status_2"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

eligible = (
    ranked
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "opened_date",
        "status",
        "balance_band",
        "last_known_status",
        "prev_balance",
        "last_known_status_2",
        F.stddev_samp("prev_balance").over(w6).alias("account_type_stddev_samp_prev_balance"),
        F.avg("event_seq").over(w7).alias("event_seq_avg_2_prior"),
        F.row_number().over(w8).alias("event_seq_2"),
    )
)

result = (
    scored
    .rollup("status")
    .agg(
        F.percentile_approx("prev_balance", [0.25, 0.5, 0.75]).alias("prev_balance_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max(F.when(F.col("status") == "OPEN", F.col("account_type_stddev_samp_prev_balance"))).alias("open_max_account_type_stddev_samp_prev_balance"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("event_seq_2"))).alias("open_total_event_seq_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("open_total_event_seq_2").desc_nulls_first())
)
