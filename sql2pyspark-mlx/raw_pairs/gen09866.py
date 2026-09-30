from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("MONTH", "paid_at")).orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("order_id_stddev_samp_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status", "order_id").orderBy("order_id_stddev_samp_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("method").orderBy(F.col("order_id_stddev_samp_amount").desc(), "id")
w9 = Window.partitionBy("status").orderBy("amount_avg_5", "id")

metrics = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.stddev_samp("amount").over(w).alias("order_id_stddev_samp_amount"),
        F.avg("amount").over(w2).alias("amount_avg_2"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
        F.last("method", ignorenulls=True).over(w4).alias("last_known_method"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "status",
        "order_id_stddev_samp_amount",
        F.sum("amount_avg_5").over(w5).alias("cumulative_amount_avg_5"),
        F.sum("amount_avg_5").over(w6).alias("cumulative_amount_avg_5_2"),
        F.avg("order_id_stddev_samp_amount").over(w7).alias("status_avg_order_id_stddev_samp_amount"),
        F.row_number().over(w8).alias("event_seq"),
        F.lag("order_id_stddev_samp_amount", 1).over(w9).alias("prev_order_id_stddev_samp_amount"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersect(
        df_payments
        .filter(F.col("order_id").isNull())
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("event_seq") >= 6)
    .select(
        "id",
        "status",
        "status_avg_order_id_stddev_samp_amount",
        "event_seq",
        F.initcap(F.trim("status")).alias("status_title"),
        F.upper("status").alias("status_upper"),
    )
)

filtered = (
    cleaned
    .filter(~F.col("status").isin(["SUCCESS", "FAILED"]))
    .select(
        "id",
        "status",
        "event_seq",
        "status_title",
        "status_upper",
        F.initcap(F.trim("status")).alias("status_title_2"),
    )
)

base = (
    filtered
    .filter(F.col("event_seq") != 6)
    .select(
        "id",
        "status",
        "status_upper",
        "status_title_2",
        F.when(F.col("event_seq") >= 7, "High").when(F.col("event_seq") >= 4, "Medium").otherwise("Low").alias("event_seq_band"),
    )
)

result = (
    base
    .groupBy("status")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
)
