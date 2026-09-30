from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("plan").orderBy("last3_max_running_avg_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
    )
)

scored_subscriptions = (
    scored
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        F.count("running_avg_monthly_fee").over(w_part).alias("last3_count_running_avg_monthly_fee"),
        F.max("running_avg_monthly_fee").over(w_part).alias("last3_max_running_avg_monthly_fee"),
        F.min("running_avg_monthly_fee").over(w_part).alias("last3_min_running_avg_monthly_fee"),
    )
)

scored_subscriptions_2 = (
    scored_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select(
        "*",
        F.avg("last3_count_running_avg_monthly_fee").over(w_recent).alias("running_avg_last3_count_running_avg_monthly_fee"),
        F.max("last3_count_running_avg_monthly_fee").over(w_recent).alias("running_max_last3_count_running_avg_monthly_fee"),
        F.count("last3_count_running_avg_monthly_fee").over(w_recent).alias("running_count_last3_count_running_avg_monthly_fee"),
        F.min("last3_count_running_avg_monthly_fee").over(w_recent).alias("running_min_last3_count_running_avg_monthly_fee"),
    )
)

cleaned = (
    scored_subscriptions_2
    .filter(F.col("end_date") >= F.lit("2023-04-14").cast("date"))
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        "last3_max_running_avg_monthly_fee",
        "last3_min_running_avg_monthly_fee",
        "running_avg_last3_count_running_avg_monthly_fee",
        "running_max_last3_count_running_avg_monthly_fee",
        "running_count_last3_count_running_avg_monthly_fee",
        "running_min_last3_count_running_avg_monthly_fee",
        F.least("running_avg_last3_count_running_avg_monthly_fee", "running_min_last3_count_running_avg_monthly_fee").alias("least_running_avg_last3_count_running_avg_monthly_fee_running_min_last3_count_running_avg_monthly_fee"),
    )
)

base = (
    cleaned
    .filter(F.col("end_date") >= F.lit("2023-05-12").cast("date"))
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        "running_count_last3_count_running_avg_monthly_fee",
        "running_min_last3_count_running_avg_monthly_fee",
        "least_running_avg_last3_count_running_avg_monthly_fee_running_min_last3_count_running_avg_monthly_fee",
        F.round((F.col("running_max_last3_count_running_avg_monthly_fee") / F.col("running_avg_last3_count_running_avg_monthly_fee")), 2).alias("running_max_last3_count_running_avg_monthly_fee_per_running_avg_last3_count_running_avg_monthly_fee"),
    )
)

result = (
    base
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        "running_count_last3_count_running_avg_monthly_fee",
        "running_min_last3_count_running_avg_monthly_fee",
        "least_running_avg_last3_count_running_avg_monthly_fee_running_min_last3_count_running_avg_monthly_fee",
        "running_max_last3_count_running_avg_monthly_fee_per_running_avg_last3_count_running_avg_monthly_fee",
    )
    .orderBy(
        F.col("least_running_avg_last3_count_running_avg_monthly_fee_running_min_last3_count_running_avg_monthly_fee").desc_nulls_first(),
        "end_date",
    )
)
