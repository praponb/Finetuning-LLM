from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("merchant")
w_hist = Window.partitionBy("txn_type")
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w_ordered = Window.orderBy("txn_time", "id")
w6 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("merchant_count_amount").desc_nulls_last(), "id")
w7 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w8 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w9 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("merchant_count_amount").desc(), "id")
w_part = Window.partitionBy("merchant", "txn_type").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w13 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_first())

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-09-26 10:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat("txn_type", F.lit(", "), "merchant").alias("txn_type_merchant"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_5_prior"),
        F.count("amount").over(w2).alias("merchant_count_amount"),
        F.max("amount").over(w_hist).alias("txn_type_max_amount"),
        F.ntile(5).over(w4).alias("amount_bucket"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .withColumn("next_amount_avg_5_prior", F.lead("amount_avg_5_prior", 1).over(w7))
    .withColumn("txn_type_max_amount_avg_90d", F.avg("txn_type_max_amount").over(w8))
    .withColumn("seq_no", F.row_number().over(w9))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w_part))
    .withColumn("last_known_amount_bucket", F.last("amount_bucket", ignorenulls=True).over(w11))
)

flagged = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w12).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w12) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("is_new_burst").over(w11))

with_gaps = (
    sessions_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w12) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w11))

ranked = (
    sessionized
    .withColumn("rank_in_group_2", F.rank().over(w13))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

result = (
    ranked
    .select("id", "txn_type", "last_known_amount", "amount")
    .unpivot(
        ["id", "txn_type"],
        ["last_known_amount", "amount"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
