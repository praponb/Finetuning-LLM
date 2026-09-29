from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("last3_min_amount").desc_nulls_last(), "id")
w3 = Window.partitionBy("account_type", "account_id").orderBy("last3_max_amount", "id")
w4 = Window.partitionBy("account_type").orderBy("latest_rank", "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-07-08").cast("date")) & (F.col("amount") <= 1700))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.round("amount", 0).alias("amount_rounded"),
    )
)

scored = (
    valid
    .filter(F.col("account_id").isin([10, 21, 23]))
    .select(
        "*",
        F.count("amount").over(w_running).alias("last3_count_amount"),
        F.avg("amount").over(w_running).alias("last3_avg_amount"),
        F.max("amount").over(w_running).alias("last3_max_amount"),
        F.min("amount").over(w_running).alias("last3_min_amount"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.txn_time",
        "e.amount_rounded",
        "e.last3_count_amount",
        "e.last3_max_amount",
        "e.last3_min_amount",
        F.col("a.balance"),
        F.col("a.account_type"),
    )
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "*",
        F.lag("last3_min_amount").over(w4).alias("prev_last3_min_amount"),
        F.lag("amount_rounded").over(w4).alias("prev_amount_rounded"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
