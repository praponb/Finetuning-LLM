from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w_hist = Window.orderBy("start_date", "id")
w5 = Window.orderBy("running_monthly_fee", "id")
w6 = Window.orderBy(F.col("running_monthly_fee").desc(), "id")
w_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("next_event_seq").desc(), "id")

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") >= F.lit("2023-09-05").cast("date")))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("monthly_fee") >= 234, "High").when(F.col("monthly_fee") >= 49, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)

scored = (
    base
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

scored_subscriptions = (
    scored
    .withColumn("next_event_seq", F.lead("event_seq", 2).over(w_hist))
    .withColumn("running_monthly_fee_bucket", F.ntile(4).over(w5))
    .withColumn("cumulative_last_known_monthly_fee", F.sum("last_known_monthly_fee").over(w))
    .withColumn("running_monthly_fee_bucket_2", F.ntile(10).over(w6))
)

with_windows = (
    scored_subscriptions
    .withColumn("running_count_monthly_fee", F.count("monthly_fee").over(w_running))
    .withColumn("running_sum_monthly_fee", F.sum("monthly_fee").over(w_running))
    .withColumn("running_max_monthly_fee", F.max("monthly_fee").over(w_running))
    .withColumn("seq_no", F.row_number().over(w8))
)

result = (
    with_windows
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "last_known_monthly_fee",
        "running_monthly_fee",
        "event_seq",
        "next_event_seq",
        "running_monthly_fee_bucket",
    )
)
