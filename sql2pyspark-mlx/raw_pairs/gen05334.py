from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy(F.col("amount_int").desc_nulls_first())
w2 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("txn_time"), "txn_type").orderBy("amount_int")
w4 = Window.partitionBy("merchant").orderBy(F.col("amount_int").desc(), "id")
w5 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.orderBy("amount_int", "id")
w7 = Window.partitionBy(F.date_trunc("WEEK", "txn_time")).orderBy("amount_int_rank_sum_90d", "id")
w_part = Window.orderBy("txn_time", "id")

base = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Shell"))) & F.col("account_id").isin([11, 36]))
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

enriched = (
    base
    .select(
        "id",
        "txn_time",
        "merchant",
        "amount_int",
        "merchant_filled",
        F.percent_rank().over(w).alias("amount_int_percent_rank"),
        F.sum("amount_int").over(w2).alias("cumulative_amount_int"),
        F.rank().over(w3).alias("amount_int_rank"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "txn_time",
        "amount_int",
        "merchant_filled",
        "amount_int_percent_rank",
        "cumulative_amount_int",
        "amount_int_rank",
    )
)

base_transactions = (
    deduped
    .filter(F.col("txn_time") > F.lit("2023-10-17").cast("date"))
    .withColumn("cumulative_amount_int_minus_amount_int", (F.col("cumulative_amount_int") - F.col("amount_int")))
)

cleaned = (
    base_transactions
    .filter(F.col("txn_time") >= F.lit("2023-10-17 19:00:00").cast("timestamp"))
    .select(
        "id",
        "txn_time",
        "amount_int",
        "merchant_filled",
        "amount_int_percent_rank",
        "amount_int_rank",
        "cumulative_amount_int_minus_amount_int",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.sum("amount_int_rank").over(w5).alias("amount_int_rank_sum_90d"),
        F.lead("amount_int_rank", 1).over(w6).alias("next_amount_int_rank"),
    )
)

with_windows_transactions = (
    with_windows
    .select(
        "id",
        "merchant_filled",
        "amount_int_percent_rank",
        "amount_int_rank_sum_90d",
        F.ntile(3).over(w7).alias("amount_int_rank_sum_90d_bucket"),
        F.lead("amount_int_percent_rank", 1).over(w_part).alias("next_amount_int_percent_rank"),
    )
)

result = (
    with_windows_transactions
    .select(
        "id",
        "merchant_filled",
        "amount_int_percent_rank",
        "amount_int_rank_sum_90d",
        "amount_int_rank_sum_90d_bucket",
    )
    .orderBy(F.col("amount_int_rank_sum_90d_bucket").desc_nulls_last())
)
