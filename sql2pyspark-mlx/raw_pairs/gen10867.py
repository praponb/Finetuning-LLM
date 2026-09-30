from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status")
w_recent = Window.orderBy(F.col("end_date").desc(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("last_known_monthly_fee").desc_nulls_last(), "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share_2"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

filtered = (
    with_windows
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-05-07")))
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_int",
        "last_known_monthly_fee",
        "monthly_fee_share_2",
        "event_seq",
        F.round((F.col("monthly_fee_int") * 1.15), 2).alias("monthly_fee_int_scaled"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest
    .filter((F.col("end_date") >= F.lit("2023-07-10").cast("date")) & (F.col("status") == "PAUSED"))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

result = (
    cleaned
    .groupBy("status")
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("last_known_monthly_fee", [0.25, 0.5, 0.75]).alias("last_known_monthly_fee_quartiles"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("event_count") > 1)
)
