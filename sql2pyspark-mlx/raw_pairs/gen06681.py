from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time_date_2").desc_nulls_last(), "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("amount_filled", "id")

filtered = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "DEBIT"]))
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        F.to_date("txn_time").alias("txn_time_date"),
        F.to_date("txn_time").alias("txn_time_date_2"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered_transactions = (
    ranked
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Grab"))))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.round("amount", 1).alias("amount_rounded"),
    )
)

with_windows = (
    filtered_transactions
    .select(
        "*",
        F.min("amount_filled").over(w2).alias("last7_min_amount_filled"),
        F.count("amount_filled").over(w2).alias("last7_count_amount_filled"),
        F.avg("amount_filled").over(w2).alias("last7_avg_amount_filled"),
        F.sum("amount_filled").over(w2).alias("last7_sum_amount_filled"),
    )
)

result = (
    with_windows
    .filter(F.col("last7_count_amount_filled") >= 13)
    .withColumn("next_last7_sum_amount_filled", F.lead("last7_sum_amount_filled").over(w3))
    .withColumn("next_amount", F.lead("amount").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)
