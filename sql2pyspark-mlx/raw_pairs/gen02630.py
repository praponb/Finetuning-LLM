from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("paid_at"), "status").orderBy("paid_at", "id")
w2 = Window.partitionBy(F.to_date("paid_at"), "method").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("amount", "id")
w4 = Window.partitionBy("order_id").orderBy("amount", "id")
w5 = Window.partitionBy("method").orderBy("event_seq", "id")
w6 = Window.partitionBy("method").orderBy(F.col("next_amount").desc(), "id")
w7 = Window.partitionBy("method").orderBy("next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("method").orderBy(F.col("next_amount").desc_nulls_last(), "id")
w9 = Window.partitionBy("method").orderBy("latest_rank")
w10 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.partitionBy("method").orderBy("next_amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("method").orderBy(F.col("last3_count_next_amount_bucket").desc_nulls_last(), "id")

enriched = (
    df_payments
    .filter(F.col("order_id").isin([12, 24, 37]))
    .select(
        "id",
        "method",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.lead("amount", 1).over(w2).alias("next_amount_2"),
        F.ntile(5).over(w3).alias("amount_bucket"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "method",
        "amount",
        "next_amount",
        F.lag("next_amount", 1, 0).over(w5).alias("prev_next_amount"),
        F.ntile(5).over(w6).alias("next_amount_bucket"),
        F.first("amount", ignorenulls=True).over(w7).alias("first_known_amount"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

latest_payments = (
    latest
    .withColumn("rank_in_group", F.rank().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest_payments
    .withColumn("last3_max_next_amount_bucket", F.max("next_amount_bucket").over(w10))
    .withColumn("last3_min_next_amount_bucket", F.min("next_amount_bucket").over(w10))
    .withColumn("last3_count_next_amount_bucket", F.count("next_amount_bucket").over(w10))
    .withColumn("last3_sum_next_amount_bucket", F.sum("next_amount_bucket").over(w10))
    .withColumn("running_prev_next_amount", F.sum("prev_next_amount").over(w11))
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w12))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    deduped
    .filter(F.col("next_amount_bucket") < 4)
    .select(
        "id",
        "method",
        "amount",
        "prev_next_amount",
        "latest_rank",
        "rank_in_group",
        "last3_max_next_amount_bucket",
        "last3_min_next_amount_bucket",
        "last3_count_next_amount_bucket",
        "last3_sum_next_amount_bucket",
        F.round("last3_count_next_amount_bucket", 2).alias("last3_count_next_amount_bucket_rounded"),
    )
)

result = (
    cleaned
    .groupBy("method")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.percentile_approx("prev_next_amount", [0.25, 0.5, 0.75]).alias("prev_next_amount_quartiles"),
    )
    .orderBy(F.col("cash_events").asc_nulls_last())
)
