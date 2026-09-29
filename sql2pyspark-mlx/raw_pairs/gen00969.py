from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("end_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("adjusted_monthly_fee", "id")
w4 = Window.partitionBy("plan").orderBy("running_monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy(F.month("end_date"), "customer_id").orderBy("adjusted_monthly_fee", "id")
w6 = Window.partitionBy("customer_id")
w7 = Window.orderBy("start_date", "id")
w8 = Window.partitionBy("customer_id").orderBy(F.col("event_seq").desc(), "id")
w_hist_2 = Window.partitionBy("customer_id").orderBy(F.col("adjusted_monthly_fee").desc())

filtered = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-05-10").cast("date")) & (F.col("monthly_fee") > 150))
    .withColumn("plan_status_key", F.concat_ws("-", "plan", "status"))
)

filtered_subscriptions = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "plan_status_key",
        F.round((F.col("monthly_fee") * 100), 2).alias("adjusted_monthly_fee"),
    )
)

with_windows = (
    filtered_subscriptions
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w2))
    .withColumn("adjusted_monthly_fee_bucket", F.ntile(5).over(w3))
)

metrics = (
    with_windows
    .select(
        "id",
        "customer_id",
        "start_date",
        "plan_status_key",
        "adjusted_monthly_fee",
        "event_seq",
        "adjusted_monthly_fee_bucket",
        F.avg("adjusted_monthly_fee").over(w4).alias("adjusted_monthly_fee_avg_6"),
        F.lag("monthly_fee", 1, 0).over(w5).alias("prev_monthly_fee"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.start_date",
        "s.adjusted_monthly_fee",
        "s.event_seq",
        F.col("c.age"),
    )
)

metrics_subscriptions = (
    with_dims
    .filter(F.col("event_seq") <= 3)
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w6)), 4))
    .withColumn("prev_age", F.lag("age", 1).over(w7))
    .withColumn("event_seq_bucket", F.ntile(3).over(w8))
    .withColumn("adjusted_monthly_fee_rank", F.rank().over(w_hist_2))
)

daily = (
    metrics_subscriptions
    .groupBy(F.date_trunc("QUARTER", "start_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("event_seq_bucket", 0.95).alias("event_seq_bucket_p95"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("event_seq_bucket_p95") >= 0)
)

result = (
    daily
    .select("quarter_start", "event_seq_bucket_p95", "num_rows")
    .orderBy(F.col("event_seq_bucket_p95").desc_nulls_last())
)
