from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.month("txn_time")).orderBy("amount", "id")
w3 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("merchant")
w5 = Window.partitionBy(F.date_trunc("YEAR", "txn_time_date"), "merchant").orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("amount_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.year("txn_time_date"), "merchant").orderBy("amount_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-06-21").cast("date"))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat_ws("/", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("account_id").isin([9, 17, 26]))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_clean",
        F.to_date("txn_time").alias("txn_time_date"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "merchant",
        "is_flagged",
        "txn_time_date",
        F.avg("amount").over(w).alias("amount_avg_3"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "merchant",
        "is_flagged",
        "amount_avg_3",
        "amount_bucket",
        "cumulative_amount",
        F.count("amount_bucket").over(w_recent).alias("merchant_count_amount_bucket"),
        F.sum("cumulative_amount").over(w5).alias("cumulative_cumulative_amount"),
        F.avg("cumulative_amount").over(w_recent).alias("merchant_avg_cumulative_amount"),
        F.sum("cumulative_amount").over(w6).alias("cumulative_cumulative_amount_2"),
        F.sum("amount_bucket").over(w7).alias("running_amount_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "merchant",
        "is_flagged",
        "amount_avg_3",
        "amount_bucket",
        "cumulative_amount",
        "merchant_count_amount_bucket",
        "cumulative_cumulative_amount",
        "merchant_avg_cumulative_amount",
        F.when(F.col("merchant").rlike("^(Starbucks|Amazon)$"), "GROUP_B").otherwise("PRIORITY").alias("merchant_zone"),
    )
    .orderBy(F.col("amount_bucket").desc_nulls_last(), F.col("merchant").desc())
)
