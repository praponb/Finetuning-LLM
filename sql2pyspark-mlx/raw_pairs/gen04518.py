from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("session_start_flag").desc(), "id")
w4 = Window.partitionBy(F.to_date("txn_time"))
w5 = Window.partitionBy("merchant").orderBy("txn_time_period", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("txn_time_period").desc(), "id")
w7 = Window.orderBy("amount_avg_5", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("DIRECT")))
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

deduped = (
    islands
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "txn_type", "amount", "txn_time", "merchant", "merchant_clean")
)

cleaned = (
    deduped
    .filter(~F.col("merchant").eqNullSafe(F.lit("Starbucks")))
    .withColumn("txn_time_period", F.date_trunc("MONTH", "txn_time"))
)

metrics = (
    cleaned
    .filter(F.col("txn_time") >= "2023-08-25")
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant_clean",
        "txn_time_period",
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
        F.avg("amount").over(w5).alias("amount_avg_5"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "amount",
        "txn_time_period",
        "seq_no",
        F.min("amount_share").over(w7).alias("last3_min_amount_share"),
        F.max("amount_share").over(w7).alias("last3_max_amount_share"),
        F.avg("amount_share").over(w7).alias("last3_avg_amount_share"),
        F.sum("amount_avg_5").over(w8).alias("amount_avg_5_sum_90d"),
    )
)
