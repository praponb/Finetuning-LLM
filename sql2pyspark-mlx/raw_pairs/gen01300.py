from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("running_max_monthly_fee_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("running_min_monthly_fee").desc_nulls_first())
w4 = Window.orderBy("running_min_monthly_fee_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("running_avg_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_running_min_monthly_fee", "id")
w7 = Window.orderBy(F.col("running_avg_monthly_fee").desc(), "id")
w8 = Window.orderBy(F.col("running_avg_monthly_fee_int").desc(), "id")

scored = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "status",
        F.avg("monthly_fee").over(w_recent).alias("running_avg_monthly_fee"),
        F.max("monthly_fee").over(w_recent).alias("running_max_monthly_fee"),
        F.min("monthly_fee").over(w_recent).alias("running_min_monthly_fee"),
        F.count("monthly_fee").over(w_recent).alias("running_count_monthly_fee"),
    )
)

valid = (
    scored
    .filter(F.col("end_date") >= F.lit("2023-08-04").cast("date"))
    .select(
        "id",
        "running_avg_monthly_fee",
        "running_min_monthly_fee",
        "running_count_monthly_fee",
        F.least("running_count_monthly_fee", "running_avg_monthly_fee").alias("least_running_count_monthly_fee_running_avg_monthly_fee"),
        F.round("running_max_monthly_fee", 0).alias("running_max_monthly_fee_rounded"),
    )
)

filtered = (
    valid
    .filter(F.col("least_running_count_monthly_fee_running_avg_monthly_fee") <= 42)
    .select(
        "id",
        "running_avg_monthly_fee",
        "running_min_monthly_fee",
        "running_count_monthly_fee",
        "least_running_count_monthly_fee_running_avg_monthly_fee",
        "running_max_monthly_fee_rounded",
        F.ceil("running_max_monthly_fee_rounded").alias("running_max_monthly_fee_rounded_whole"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "running_avg_monthly_fee",
        "least_running_count_monthly_fee_running_avg_monthly_fee",
        F.sum("running_min_monthly_fee").over(w2).alias("running_running_min_monthly_fee"),
        F.cume_dist().over(w3).alias("running_min_monthly_fee_cume_dist"),
    )
)

scored_subscriptions = (
    enriched
    .select(
        "id",
        "running_avg_monthly_fee",
        "running_running_min_monthly_fee",
        F.first("running_avg_monthly_fee", ignorenulls=True).over(w4).alias("first_known_running_avg_monthly_fee"),
        F.last("running_avg_monthly_fee", ignorenulls=True).over(w_hist).alias("last_known_running_avg_monthly_fee"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

filtered_subscriptions = (
    scored_subscriptions
    .filter(F.col("running_avg_monthly_fee") < 110)
    .select(
        "*",
        F.col("running_avg_monthly_fee").cast("int").alias("running_avg_monthly_fee_int"),
    )
)

metrics = (
    filtered_subscriptions
    .select(
        "*",
        F.row_number().over(w7).alias("row_seq_2"),
        F.ntile(10).over(w8).alias("running_avg_monthly_fee_int_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "running_avg_monthly_fee",
        "last_known_running_avg_monthly_fee",
        "row_seq",
        "running_avg_monthly_fee_int",
        "row_seq_2",
        "running_avg_monthly_fee_int_bucket",
    )
    .orderBy(F.col("running_avg_monthly_fee_int").desc())
)
