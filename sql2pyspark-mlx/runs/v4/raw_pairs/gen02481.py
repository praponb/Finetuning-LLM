from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("start_date", "id")
w3 = Window.partitionBy("status", "plan")
w4 = Window.partitionBy("status").orderBy("monthly_fee_rounded", "id")
w5 = Window.orderBy("monthly_fee_rounded")
w6 = Window.partitionBy("plan", "status").orderBy(F.col("start_date").asc_nulls_last(), "id")
w7 = Window.orderBy("monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("monthly_fee_rounded_percent_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("monthly_fee_rounded_bucket").desc_nulls_first())

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-09-15").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
    )
)

base_subscriptions = (
    base
    .filter(F.col("start_date") > F.to_date(F.lit("2024-03-28")))
    .withColumn("monthly_fee_rounded_tier", F.when(F.col("monthly_fee_rounded") >= 81, "High").when(F.col("monthly_fee_rounded") >= 28, "Medium").otherwise("Low"))
)

with_windows = (
    base_subscriptions
    .withColumn("running_monthly_fee_rounded", F.sum("monthly_fee_rounded").over(w))
    .withColumn("next_monthly_fee_rounded", F.lead("monthly_fee_rounded", 1).over(w2))
    .withColumn("status_max_monthly_fee_rounded", F.max("monthly_fee_rounded").over(w3))
    .withColumn("monthly_fee_rounded_bucket", F.ntile(5).over(w4))
    .withColumn("monthly_fee_rounded_percent_rank", F.percent_rank().over(w5))
)

filtered = (
    with_windows
    .filter((F.col("start_date") >= F.lit("2023-11-13").cast("date")) & (F.col("monthly_fee_rounded_bucket") >= 4))
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        "monthly_fee_rounded_tier",
        "running_monthly_fee_rounded",
        "status_max_monthly_fee_rounded",
        "monthly_fee_rounded_bucket",
        "monthly_fee_rounded_percent_rank",
        F.initcap(F.trim("status")).alias("status_title"),
        (F.col("monthly_fee_rounded_percent_rank") + F.col("monthly_fee_rounded_bucket")).alias("monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

valid = (
    latest
    .filter(F.col("status_max_monthly_fee_rounded").between(37, 77))
    .select(
        "id",
        "start_date",
        "monthly_fee_rounded_tier",
        "monthly_fee_rounded_bucket",
        "monthly_fee_rounded_percent_rank",
        "monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket",
        "rn",
        (F.col("monthly_fee_rounded_percent_rank") - F.col("monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket")).alias("monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket"),
        F.lower("plan").alias("plan_lower"),
    )
)

with_windows_subscriptions = (
    valid
    .select(
        "*",
        F.lag("monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket", 1).over(w2).alias("prev_monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket"),
        F.first("monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket", ignorenulls=True).over(w7).alias("first_known_monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket"),
        F.last("monthly_fee_rounded_percent_rank", ignorenulls=True).over(w8).alias("last_known_monthly_fee_rounded_percent_rank"),
        F.percent_rank().over(w9).alias("monthly_fee_rounded_percent_rank_percent_rank"),
        F.percent_rank().over(w10).alias("monthly_fee_rounded_bucket_percent_rank"),
    )
)

daily = (
    with_windows_subscriptions
    .groupBy(
        F.when(F.col("last_known_monthly_fee_rounded_percent_rank") >= 0.75, "HIGH").otherwise("LOW").alias("last_known_monthly_fee_rounded_percent_rank_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("monthly_fee_rounded_percent_rank_percent_rank", [0.25, 0.5, 0.75]).alias("monthly_fee_rounded_percent_rank_percent_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("monthly_fee_rounded_percent_rank_percent_rank", 0.9).alias("monthly_fee_rounded_percent_rank_percent_rank_p90"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    daily
    .select(
        "last_known_monthly_fee_rounded_percent_rank_tier",
        "distinct_ids",
        "monthly_fee_rounded_percent_rank_percent_rank_quartiles",
        "distinct_ids_2",
        "monthly_fee_rounded_percent_rank_percent_rank_p90",
    )
    .orderBy(
        F.col("last_known_monthly_fee_rounded_percent_rank_tier").desc_nulls_last(),
    )
)
