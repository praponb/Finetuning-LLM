from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("burst_id").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("method").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")
w6 = Window.partitionBy("status", "method").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-09-14 08:00:00").cast("timestamp")) & (F.col("order_id") == 6))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.trim("method").alias("clean_method"),
    )
)

with_prev = cleaned.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "clean_method",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

scored = (
    sessions_payments
    .select(
        "*",
        F.ntile(4).over(w3).alias("burst_id_bucket"),
        F.avg("burst_id").over(w4).alias("burst_id_avg_6_prior"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.shipping_fee"), F.col("b.order_date"), F.col("b.channel"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.min("burst_id_avg_6_prior").over(w6).alias("running_min_burst_id_avg_6_prior"),
        F.count("burst_id_avg_6_prior").over(w6).alias("running_count_burst_id_avg_6_prior"),
        F.sum("burst_id_avg_6_prior").over(w6).alias("running_sum_burst_id_avg_6_prior"),
    )
)

result = (
    with_windows
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .cube(
        F.col("o.channel").alias("order_channel"),
        F.col("src.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.running_min_burst_id_avg_6_prior")).alias("total_running_min_burst_id_avg_6_prior"),
        F.grouping_id().alias("level_id"),
    )
)
