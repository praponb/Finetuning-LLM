from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-05-12").cast("date")) & F.col("merchant").isNotNull() & (F.to_date("txn_time") >= "2024-06-01"))
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

gaps = (
    latest
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

result = (
    sessions_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "FEE"])
    .agg(F.avg("session_id").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "merchant",
        F.col("REFUND_total").alias("refund_total"),
        F.col("REFUND_item_count").alias("refund_item_count"),
        F.col("FEE_total").alias("fee_total"),
        F.col("FEE_item_count").alias("fee_item_count"),
    )
)
