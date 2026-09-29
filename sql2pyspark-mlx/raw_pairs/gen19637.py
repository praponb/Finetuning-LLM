from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w_grp = Window.partitionBy("status", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("order_id", "status").orderBy("amount", "id")
w_grp_2 = Window.partitionBy("order_id").orderBy(F.col("amount_avg_5").desc(), "id")
w6 = Window.partitionBy("status").orderBy("amount_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("amount_max_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w9 = Window.partitionBy("status")
w10 = Window.partitionBy(F.year("paid_at"), "status")
w11 = Window.partitionBy("status").orderBy(F.col("avg_running_sum_first_known_amount_bucket_share").desc())

metrics = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "FAILED"]))
    .withColumn("amount_avg_5", F.avg("amount").over(w))
    .withColumn("amount_bucket", F.ntile(3).over(w2))
)

with_windows = (
    metrics
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "amount_avg_5",
        F.max("amount").over(w_grp).alias("amount_max_90d"),
        F.ntile(5).over(w4).alias("amount_bucket_2"),
        F.row_number().over(w_grp_2).alias("event_seq"),
        F.first("amount_bucket", ignorenulls=True).over(w6).alias("first_known_amount_bucket"),
    )
)

metrics_payments = (
    with_windows
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "amount_avg_5",
        "first_known_amount_bucket",
        F.max("first_known_amount_bucket").over(w7).alias("running_max_first_known_amount_bucket"),
        F.sum("first_known_amount_bucket").over(w7).alias("running_sum_first_known_amount_bucket"),
        F.min("first_known_amount_bucket").over(w7).alias("running_min_first_known_amount_bucket"),
    )
)

with_windows_payments = (
    metrics_payments
    .select(
        "*",
        F.sum("amount_avg_5").over(w8).alias("amount_avg_5_sum_90d"),
        F.round((F.col("running_sum_first_known_amount_bucket") / F.sum("running_sum_first_known_amount_bucket").over(w9)), 4).alias("running_sum_first_known_amount_bucket_share"),
        F.stddev_samp("running_max_first_known_amount_bucket").over(w10).alias("paid_at_year_stddev_samp_running_max_first_known_amount_bucket"),
    )
)

base = (
    with_windows_payments
    .filter(F.col("running_sum_first_known_amount_bucket").between(18, 33))
    .withColumn("running_sum_first_known_amount_bucket_int", F.col("running_sum_first_known_amount_bucket").cast("int"))
)

result = (
    base
    .groupBy(
        "status",
        F.when(F.col("running_max_first_known_amount_bucket") >= 8.6, "LARGE").otherwise("SMALL").alias("running_max_first_known_amount_bucket_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.round(F.avg("running_sum_first_known_amount_bucket_share"), 2).alias("avg_running_sum_first_known_amount_bucket_share"),
        F.max("paid_at").alias("last_paid_at"),
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("avg_running_sum_first_known_amount_bucket_share") > 0)
    .withColumn("avg_running_sum_first_known_amount_bucket_share_rank", F.rank().over(w11))
)
