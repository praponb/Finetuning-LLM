from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy(F.year("opened_date"), "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.date_trunc("week", "opened_date"), "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type")
w4 = Window.partitionBy(F.year("opened_date"), "customer_id").orderBy(F.col("cumulative_weeks_since_opened_date").desc())
w5 = Window.partitionBy("account_type", "customer_id")
w6 = Window.orderBy(F.col("account_type_min_cumulative_weeks_since_opened_date").desc_nulls_first())
w7 = Window.orderBy(F.col("cumulative_weeks_since_opened_date_dense_rank").desc_nulls_last())
w8 = Window.orderBy("cumulative_weeks_since_opened_date_dense_rank")
w9 = Window.orderBy(F.col("account_type_min_cumulative_weeks_since_opened_date_percent_rank").asc_nulls_first())
w10 = Window.orderBy(F.col("cumulative_weeks_since_opened_date_dense_rank").asc_nulls_last())

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-05-26").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.datediff(F.current_date(), "opened_date") / 7).alias("weeks_since_opened_date"),
    )
)

enriched = (
    base
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.sum("weeks_since_opened_date").over(w_grp).alias("cumulative_weeks_since_opened_date"),
        F.sum("weeks_since_opened_date").over(w_recent).alias("running_weeks_since_opened_date"),
        F.sum("weeks_since_opened_date").over(w3).alias("account_type_sum_weeks_since_opened_date"),
    )
)

kept_keys = (
    enriched
    .select("id", "customer_id")
    .exceptAll(
        df_accounts
        .filter(F.col("opened_date") < "2023-07-01")
        .select("id", "customer_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "id",
        "balance",
        "opened_date",
        F.avg("balance").over(w3).alias("account_type_avg_balance"),
        F.dense_rank().over(w4).alias("cumulative_weeks_since_opened_date_dense_rank"),
        F.sum("balance").over(w5).alias("account_type_sum_balance"),
        F.min("cumulative_weeks_since_opened_date").over(w3).alias("account_type_min_cumulative_weeks_since_opened_date"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

not_flagged_accounts = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched_accounts = (
    not_flagged_accounts
    .select(
        "id",
        "account_type_avg_balance",
        "cumulative_weeks_since_opened_date_dense_rank",
        "account_type_sum_balance",
        "account_type_min_cumulative_weeks_since_opened_date",
        F.percent_rank().over(w6).alias("account_type_min_cumulative_weeks_since_opened_date_percent_rank"),
        F.cume_dist().over(w7).alias("cumulative_weeks_since_opened_date_dense_rank_cume_dist"),
        F.cume_dist().over(w8).alias("cumulative_weeks_since_opened_date_dense_rank_cume_dist_2"),
    )
)

result = (
    enriched_accounts
    .select(
        "id",
        "cumulative_weeks_since_opened_date_dense_rank",
        "account_type_min_cumulative_weeks_since_opened_date",
        "account_type_min_cumulative_weeks_since_opened_date_percent_rank",
        "cumulative_weeks_since_opened_date_dense_rank_cume_dist_2",
        F.rank().over(w9).alias("account_type_min_cumulative_weeks_since_opened_date_percent_rank_rank"),
        F.cume_dist().over(w10).alias("cumulative_weeks_since_opened_date_dense_rank_cume_dist_3"),
    )
)
