from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.month("opened_date"), "status").orderBy(F.col("balance").desc(), "id")
w_running = Window.partitionBy(F.year("opened_date"), "status").orderBy(F.col("balance_bucket").desc(), "id")
w4 = Window.orderBy("prev_balance", "id")
w5 = Window.partitionBy("account_type").orderBy("row_seq", "id")

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-11"))) & F.col("balance").between(22000, 47000))
    .select("*", F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"))
)

metrics = (
    filtered
    .select(
        "*",
        F.lag("balance").over(w).alias("prev_balance"),
        F.lead("balance").over(w).alias("next_balance"),
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w2).alias("balance_bucket"),
    )
)

with_windows = (
    metrics
    .filter(F.col("next_balance") < 66000)
    .select(
        "*",
        F.ntile(3).over(w_running).alias("balance_bucket_bucket"),
        F.lag("next_balance", 2).over(w4).alias("prev_next_balance"),
    )
)

with_windows_accounts = (
    with_windows
    .select(
        "id",
        "balance",
        "opened_date",
        "opened_date_label",
        "prev_balance",
        "row_seq",
        "balance_bucket",
        "balance_bucket_bucket",
        F.lag("balance").over(w5).alias("prev_balance_2"),
        F.lead("balance").over(w5).alias("next_balance_2"),
        F.lead("balance_bucket").over(w5).alias("next_balance_bucket"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

cleaned = (
    with_windows_accounts
    .filter(F.col("opened_date") >= F.lit("2023-05-11").cast("date"))
    .select(
        "id",
        "balance",
        "opened_date",
        "prev_balance",
        "row_seq",
        "balance_bucket",
        "prev_balance_2",
        "next_balance_2",
        "next_balance_bucket",
        "event_seq",
        F.floor("next_balance_2").alias("next_balance_2_whole"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        F.when(F.col("balance_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("balance_bucket_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.percentile_approx("prev_balance_2", [0.25, 0.5, 0.75]).alias("prev_balance_2_quartiles"),
    )
    .orderBy(F.col("balance_bucket_tier").desc_nulls_last())
)
