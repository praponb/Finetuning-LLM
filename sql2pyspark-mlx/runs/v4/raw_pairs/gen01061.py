from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("account_type")
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("opened_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("opened_date_dayofweek_avg_5").desc(), "id")
w6 = Window.partitionBy("status").orderBy("balance", "id")
w7 = Window.orderBy("opened_date_dayofweek", "id").rowsBetween(-2, -1)
w8 = Window.orderBy("status_stddev_samp_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull() & ~F.col("account_type").isin(["CREDIT", "CHECKING", "LOAN"]))
    .select(
        "*",
        F.abs("balance").alias("abs_balance"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

base = (
    filtered
    .filter((F.col("opened_date") > F.lit("2024-02-13").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "resolved_status",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status_2"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.stddev_samp("balance").over(w).alias("status_stddev_samp_balance"),
        F.min("opened_date_dayofweek").over(w2).alias("account_type_min_opened_date_dayofweek"),
        F.avg("opened_date_dayofweek").over(w3).alias("opened_date_dayofweek_avg_5"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_semi")
)

scored = (
    eligible
    .select(
        "*",
        F.sum("opened_date_dayofweek_avg_5").over(w4).alias("running_opened_date_dayofweek_avg_5"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("row_seq"),
        F.avg("opened_date_dayofweek_avg_5").over(w7).alias("opened_date_dayofweek_avg_5_avg_2_prior"),
        F.sum("status_stddev_samp_balance").over(w8).alias("cumulative_status_stddev_samp_balance"),
    )
)

eligible_accounts = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    eligible_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "resolved_status",
        "status_prefix",
        "opened_date_dayofweek",
        "account_type_min_opened_date_dayofweek",
        F.when(F.col("status").rlike("^(OPEN|FROZEN)$"), "CORE").when(F.col("status").rlike("^(CLOSED)$"), "PRIORITY").otherwise("GROUP_A").alias("status_zone"),
    )
)
