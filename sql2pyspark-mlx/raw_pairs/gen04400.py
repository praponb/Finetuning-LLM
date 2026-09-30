from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("paid_at"), "status").orderBy("days_since_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "status").orderBy("paid_at", "id")
w_hist = Window.partitionBy(F.month("paid_at")).orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("method")
w5 = Window.partitionBy("status", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy(F.col("prev_days_since_paid_at").desc(), "id")

cleaned = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("promptpay"))) & (F.col("paid_at") > F.lit("2023-09-14 10:00:00").cast("timestamp")))
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label_2"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("method").isNotNull())
    .withColumn("days_since_paid_at", F.datediff(F.current_date(), "paid_at"))
    .withColumn("method_status_key", F.concat_ws("-", "method", "status"))
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("days_since_paid_at").over(w).alias("running_days_since_paid_at"),
        F.lag("days_since_paid_at", 1).over(w2).alias("prev_days_since_paid_at"),
        F.sum("days_since_paid_at").over(w_hist).alias("running_days_since_paid_at_2"),
        F.count("days_since_paid_at").over(w_running).alias("method_count_days_since_paid_at"),
    )
)

metrics = (
    with_windows
    .filter(F.col("method") != "promptpay")
    .select(
        "id",
        "paid_at_label",
        "paid_at_label_2",
        "method_clean",
        "days_since_paid_at",
        "running_days_since_paid_at",
        "running_days_since_paid_at_2",
        "method_count_days_since_paid_at",
        F.avg("running_days_since_paid_at").over(w5).alias("running_days_since_paid_at_avg_7d"),
        F.ntile(10).over(w6).alias("prev_days_since_paid_at_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("running_days_since_paid_at_2") >= 367, "PREMIUM").otherwise("STANDARD").alias("running_days_since_paid_at_2_tier"),
    )
    .agg(
        F.avg("prev_days_since_paid_at_bucket").alias("avg_prev_days_since_paid_at_bucket"),
        F.percentile_approx("method_count_days_since_paid_at", [0.25, 0.5, 0.75]).alias("method_count_days_since_paid_at_quartiles"),
        F.round(F.avg("running_days_since_paid_at"), 2).alias("avg_running_days_since_paid_at"),
    )
    .orderBy(F.col("avg_running_days_since_paid_at").asc_nulls_last())
)
