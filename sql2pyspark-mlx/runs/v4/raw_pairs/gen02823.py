from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w_recent = Window.partitionBy("account_id")
w4 = Window.partitionBy("merchant").orderBy("txn_time_day", "id")
w5 = Window.partitionBy("merchant")
w6 = Window.partitionBy("account_id", "merchant")
w7 = Window.partitionBy("merchant", "account_id").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("amount_share", "id").rowsBetween(-5, -1)

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2024-02-08 07:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.stddev_samp("amount").over(w_recent).alias("account_id_stddev_samp_amount"),
        F.row_number().over(w4).alias("event_seq"),
        F.stddev_samp("amount").over(w5).alias("merchant_stddev_samp_amount"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w6)), 4).alias("amount_share_2"),
        F.first("account_id_stddev_samp_amount", ignorenulls=True).over(w7).alias("first_known_account_id_stddev_samp_amount"),
        F.avg("amount").over(w8).alias("amount_avg_5_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "txn_time_day",
        "txn_time_period",
        "merchant_clean",
        "amount_share",
        "account_id_stddev_samp_amount",
    )
)
