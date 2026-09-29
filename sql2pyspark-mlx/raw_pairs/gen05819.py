from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("days_since_txn_time").desc_nulls_first(), "id")
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("amount")
w4 = Window.partitionBy("account_id").orderBy("amount", "id")
w5 = Window.partitionBy("merchant").orderBy(F.col("prev_amount").desc_nulls_last())
w6 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_first())
w7 = Window.partitionBy("merchant").orderBy("prev_amount_plus_rn")
w8 = Window.partitionBy(F.date_trunc("MONTH", "txn_time"), "merchant").orderBy("prev_amount", "id")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-08-28").cast("date")) & (~F.col("merchant").eqNullSafe(F.lit("Starbucks"))))
    .select("*", F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"))
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "rn",
        F.last("merchant", ignorenulls=True).over(w2).alias("last_known_merchant"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.lag("amount", 2).over(w4).alias("prev_amount"),
    )
)

filtered = (
    enriched
    .filter(F.col("amount").between(2500, 2700) & (F.col("txn_time") >= F.to_timestamp(F.lit("2023-05-02 04:00:00"))))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "rn",
        "last_known_merchant",
        "amount_cume_dist",
        "prev_amount",
        F.round("amount", 2).alias("amount_rounded"),
        (F.col("prev_amount") + F.col("rn")).alias("prev_amount_plus_rn"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

result = (
    filtered
    .filter(F.col("amount_cume_dist").between(0.68, 0.74))
    .select(
        "id",
        "merchant",
        "merchant_clean",
        F.percent_rank().over(w5).alias("prev_amount_percent_rank"),
        F.percent_rank().over(w6).alias("amount_percent_rank"),
        F.dense_rank().over(w7).alias("prev_amount_plus_rn_dense_rank"),
        F.ntile(10).over(w8).alias("prev_amount_bucket"),
    )
)
