from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w2 = Window.partitionBy(F.month("opened_date"), "account_type").orderBy(F.col("balance").desc_nulls_last())
w3 = Window.partitionBy("status")
w4 = Window.orderBy(F.col("balance").desc(), "id")
w_part = Window.partitionBy(F.month("opened_date")).orderBy("balance", "id").rowsBetween(-3, -1)
w_hist = Window.orderBy("status_min_balance", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("account_type_count_balance", "id")
w8 = Window.orderBy(F.col("row_seq").desc(), "id")
w9 = Window.orderBy(F.col("status_min_balance_avg_3").desc(), "id")
w10 = Window.partitionBy(F.year("opened_date")).orderBy("account_type_count_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status"),
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status_2"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

enriched = (
    cleaned
    .filter(F.col("balance") <= 53000)
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "account_type_status_2",
        "status_filled",
        F.count("balance").over(w).alias("account_type_count_balance"),
        F.percent_rank().over(w2).alias("balance_percent_rank"),
        F.min("balance").over(w3).alias("status_min_balance"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "balance",
        "opened_date",
        "account_type_count_balance",
        "balance_percent_rank",
        F.row_number().over(w4).alias("row_seq"),
        F.avg("status_min_balance").over(w_part).alias("status_min_balance_avg_3_prior"),
        F.avg("status_min_balance").over(w_hist).alias("status_min_balance_avg_3"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.lag("status_min_balance_avg_3_prior", 1, 0).over(w7).alias("prev_status_min_balance_avg_3_prior"),
        F.ntile(3).over(w8).alias("row_seq_bucket"),
        F.ntile(4).over(w9).alias("status_min_balance_avg_3_bucket"),
        F.last("balance_percent_rank", ignorenulls=True).over(w10).alias("last_known_balance_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "balance",
        "opened_date",
        "status_min_balance_avg_3_prior",
        "status_min_balance_avg_3",
        "prev_status_min_balance_avg_3_prior",
        "row_seq_bucket",
        "status_min_balance_avg_3_bucket",
        "last_known_balance_percent_rank",
    )
    .orderBy(
        F.col("status_min_balance_avg_3_bucket").desc_nulls_first(),
        F.col("opened_date").desc(),
    )
)
