from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan", "status")
w4 = Window.partitionBy("plan", "status").orderBy("monthly_fee")

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-06-24").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("monthly_fee") >= 240, "High").when(F.col("monthly_fee") >= 56, "Medium").otherwise("Low").alias("monthly_fee_tier"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "start_date",
        "end_date",
        F.lag("monthly_fee", 2, 0).over(w).alias("prev_monthly_fee"),
        F.min("monthly_fee").over(w2).alias("plan_min_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4).alias("monthly_fee_share"),
        F.percent_rank().over(w4).alias("monthly_fee_percent_rank"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") < 150)
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("end_date") > F.to_date(F.lit("2023-04-29")))
    .withColumn("monthly_fee_share_times_prev_monthly_fee", (F.col("monthly_fee_share") * F.col("prev_monthly_fee")))
)

grouped = (
    filtered
    .withColumn("monthly_fee_share_tier", F.when(F.col("monthly_fee_share") >= 0.79, "HIGH").otherwise("LOW"))
    .withColumn("plan_min_monthly_fee_tier", F.when(F.col("plan_min_monthly_fee") >= 258, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["monthly_fee_share_tier", "plan_min_monthly_fee_tier"], ["monthly_fee_share_tier"], []],
        "monthly_fee_share_tier",
        "plan_min_monthly_fee_tier",
    )
    .agg(
        F.percentile_approx("monthly_fee_percent_rank", 0.9).alias("monthly_fee_percent_rank_p90"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.round(F.avg("prev_monthly_fee"), 2).alias("avg_prev_monthly_fee"),
    )
    .filter(F.col("cnt_2") > 1)
)

result = (
    grouped
    .select(
        "monthly_fee_share_tier",
        "plan_min_monthly_fee_tier",
        "monthly_fee_percent_rank_p90",
        "avg_prev_monthly_fee",
    )
    .orderBy(F.col("avg_prev_monthly_fee").desc_nulls_first())
)
