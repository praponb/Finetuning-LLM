from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("weeks_starts_on_to_ends_on", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel")
w4 = Window.orderBy("channel_count_weeks_starts_on_to_ends_on", "id")
w5 = Window.orderBy(F.col("channel_count_weeks_starts_on_to_ends_on").asc_nulls_last())
w6 = Window.orderBy("weeks_starts_on_to_ends_on")
w7 = Window.orderBy("weeks_starts_on_to_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-09-20").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        (F.datediff("ends_on", "starts_on") / 7).alias("weeks_starts_on_to_ends_on"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

scored = (
    cleaned
    .filter(F.col("ends_on") > "2025-09-12")
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "weeks_starts_on_to_ends_on",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_6_prior"),
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
        F.count("weeks_starts_on_to_ends_on").over(w3).alias("channel_count_weeks_starts_on_to_ends_on"),
    )
)

filtered = (
    scored
    .filter(F.col("ends_on") >= F.lit("2023-10-22").cast("date"))
    .select(
        "id",
        "ends_on",
        "weeks_starts_on_to_ends_on",
        "discount_pct_avg_6_prior",
        "channel_count_weeks_starts_on_to_ends_on",
        F.least("weeks_starts_on_to_ends_on", "discount_pct_dense_rank").alias("least_weeks_starts_on_to_ends_on_discount_pct_dense_rank"),
        F.least("discount_pct_avg_6_prior", "discount_pct").alias("least_discount_pct_avg_6_prior_discount_pct"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w4).alias("channel_count_weeks_starts_on_to_ends_on_bucket"),
        F.cume_dist().over(w5).alias("channel_count_weeks_starts_on_to_ends_on_cume_dist"),
        F.percent_rank().over(w6).alias("weeks_starts_on_to_ends_on_percent_rank"),
        F.sum("least_discount_pct_avg_6_prior_discount_pct").over(w7).alias("running_least_discount_pct_avg_6_prior_discount_pct"),
    )
)

result = (
    metrics
    .withColumn("activity_month", F.date_trunc("MONTH", "ends_on"))
    .withColumn("running_least_discount_pct_avg_6_prior_discount_pct_tier", F.when(F.col("running_least_discount_pct_avg_6_prior_discount_pct") >= 343, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["activity_month", "running_least_discount_pct_avg_6_prior_discount_pct_tier"], ["activity_month"], []],
        "activity_month",
        "running_least_discount_pct_avg_6_prior_discount_pct_tier",
    )
    .agg(
        F.percentile_approx("least_weeks_starts_on_to_ends_on_discount_pct_dense_rank", 0.9).alias("least_weeks_starts_on_to_ends_on_discount_pct_dense_rank_p90"),
        F.percentile_approx("least_discount_pct_avg_6_prior_discount_pct", [0.25, 0.5, 0.75]).alias("least_discount_pct_avg_6_prior_discount_pct_quartiles"),
        F.round(F.avg("weeks_starts_on_to_ends_on"), 2).alias("avg_weeks_starts_on_to_ends_on"),
        F.count("*").alias("cnt"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .filter(F.col("least_weeks_starts_on_to_ends_on_discount_pct_dense_rank_p90") > 0)
)
