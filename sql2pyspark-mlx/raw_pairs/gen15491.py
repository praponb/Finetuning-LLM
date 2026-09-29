from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w_running = Window.partitionBy("status").orderBy("status_length", "id")
w3 = Window.partitionBy(F.month("paid_at"), "status").orderBy(F.col("status_length").desc())
w4 = Window.partitionBy("status").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "order_id")
w6 = Window.partitionBy("status").orderBy("status_length", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("last4_sum_status_stddev_samp_status_length").desc_nulls_first(), "id")

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-08-02 07:00:00").cast("timestamp"))
    .select(
        "*",
        F.to_date("paid_at").alias("paid_at_date"),
        F.length("status").alias("status_length"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "status_length",
        F.lag("amount", 2).over(w).alias("prev_amount"),
        F.ntile(3).over(w_running).alias("status_length_bucket"),
        F.cume_dist().over(w3).alias("status_length_cume_dist"),
        F.sum("status_length").over(w4).alias("running_status_length"),
        F.stddev_samp("status_length").over(w5).alias("status_stddev_samp_status_length"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "status_length",
        "prev_amount",
        "status_length_bucket",
        "status_length_cume_dist",
        "running_status_length",
        F.min("status_stddev_samp_status_length").over(w6).alias("last4_min_status_stddev_samp_status_length"),
        F.count("status_stddev_samp_status_length").over(w6).alias("last4_count_status_stddev_samp_status_length"),
        F.sum("status_stddev_samp_status_length").over(w6).alias("last4_sum_status_stddev_samp_status_length"),
        F.max("status_stddev_samp_status_length").over(w6).alias("last4_max_status_stddev_samp_status_length"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.last4_max_status_stddev_samp_status_length")).alias("max_last4_max_status_stddev_samp_status_length"),
    )
)
