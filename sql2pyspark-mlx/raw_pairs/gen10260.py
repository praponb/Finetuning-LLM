from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "status").orderBy("session_no", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.partitionBy(F.to_date("paid_at"), "status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id")
w_recent = Window.partitionBy("order_id").orderBy(F.col("is_new_burst").desc(), "id")
w8 = Window.partitionBy("method").orderBy("paid_at", "id")
w9 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

gaps = (
    latest
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

metrics = (
    islands
    .withColumn("last7_sum_amount", F.sum("amount").over(w4))
    .withColumn("last7_max_amount", F.max("amount").over(w4))
    .withColumn("last7_avg_amount", F.avg("amount").over(w4))
)

with_windows = (
    metrics
    .select(
        "*",
        F.last("last7_sum_amount", ignorenulls=True).over(w_part).alias("last_known_last7_sum_amount"),
        F.sum("last7_avg_amount").over(w6).alias("order_id_sum_last7_avg_amount"),
        F.ntile(5).over(w_recent).alias("is_new_burst_bucket"),
    )
)

with_prev = with_windows.withColumn("prev_paid_at", F.lag("paid_at").over(w8))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands_payments = (
    flagged
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "is_new_burst",
        "last7_sum_amount",
        "last_known_last7_sum_amount",
        "order_id_sum_last7_avg_amount",
        F.sum("session_start_flag").over(w9).alias("session_id"),
    )
)

kept_keys = (
    islands_payments
    .select("id")
    .subtract(
        df_payments
        .filter(F.col("amount") > 990)
        .select("id")
    )
)

reconciled = (
    islands_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .groupBy(
        F.when(F.col("last7_sum_amount") >= 9000, "HIGH").otherwise("LOW").alias("last7_sum_amount_tier"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("order_id_sum_last7_avg_amount"), 2).alias("avg_order_id_sum_last7_avg_amount"),
        F.round(F.sum("last7_sum_amount") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("last7_sum_amount_per_failed"),
        F.min("paid_at").alias("started_at"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
)

result = (
    totals
    .select(
        "last7_sum_amount_tier",
        "quarter_start",
        "avg_order_id_sum_last7_avg_amount",
        "started_at",
        "num_methods",
        "method_list",
    )
    .orderBy(F.col("num_methods").desc_nulls_last())
)
