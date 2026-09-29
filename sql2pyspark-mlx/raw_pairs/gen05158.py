from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("end_date"))
w3 = Window.orderBy("running_avg_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("running_count_monthly_fee", "id")
w_hist = Window.orderBy("monthly_fee_share", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("running_count_monthly_fee").over(w3).alias("running_running_count_monthly_fee"),
        F.first("running_count_monthly_fee", ignorenulls=True).over(w4).alias("first_known_running_count_monthly_fee"),
    )
)

filtered = (
    metrics
    .filter(F.col("start_date") > F.to_date(F.lit("2023-03-26")))
    .withColumn("running_avg_monthly_fee_band", F.when(F.col("running_avg_monthly_fee") >= 212, "High").when(F.col("running_avg_monthly_fee") >= 81, "Medium").otherwise("Low"))
    .withColumn("least_running_running_count_monthly_fee_running_min_monthly_fee", F.least("running_running_count_monthly_fee", "running_min_monthly_fee"))
)

metrics_subscriptions = (
    filtered
    .select(
        "id",
        "running_min_monthly_fee",
        "running_avg_monthly_fee",
        "monthly_fee_share",
        "running_running_count_monthly_fee",
        "first_known_running_count_monthly_fee",
        "least_running_running_count_monthly_fee_running_min_monthly_fee",
        F.lead("least_running_running_count_monthly_fee_running_min_monthly_fee", 1).over(w_part).alias("next_least_running_running_count_monthly_fee_running_min_monthly_fee"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

metrics_subscriptions_2 = (
    metrics_subscriptions
    .select(
        "id",
        "monthly_fee_share",
        "first_known_running_count_monthly_fee",
        "least_running_running_count_monthly_fee_running_min_monthly_fee",
        F.avg("running_min_monthly_fee").over(w_hist).alias("last7_avg_running_min_monthly_fee"),
        F.min("running_min_monthly_fee").over(w_hist).alias("last7_min_running_min_monthly_fee"),
        F.sum("running_min_monthly_fee").over(w_hist).alias("last7_sum_running_min_monthly_fee"),
        F.max("running_min_monthly_fee").over(w_hist).alias("last7_max_running_min_monthly_fee"),
    )
)

result = (
    metrics_subscriptions_2
    .select(
        "id",
        "monthly_fee_share",
        "first_known_running_count_monthly_fee",
        "least_running_running_count_monthly_fee_running_min_monthly_fee",
        "last7_min_running_min_monthly_fee",
    )
)
