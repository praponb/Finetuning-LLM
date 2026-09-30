from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("discount_pct", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("MONTH", "starts_on")).orderBy("is_welcome5", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("is_welcome5_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("discount_pct").desc())
w6 = Window.orderBy("is_welcome5_avg_2_2", "id")
w7 = Window.orderBy("discount_pct", "id").rowsBetween(-5, -1)

base = (
    df_promotions
    .filter(~F.col("channel").eqNullSafe(F.lit("mobile")))
    .select(
        "*",
        F.date_sub("ends_on", 32).alias("ends_on_minus_32d"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

filtered = (
    base
    .filter((F.col("ends_on") > F.lit("2023-08-11").cast("date")) & F.col("channel").isNotNull())
    .withColumn("is_welcome5", F.when(F.col("promo_code") == "WELCOME5", 1).otherwise(0))
)

with_windows = (
    filtered
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "ends_on_minus_32d",
        "channel_clean",
        "is_welcome5",
        F.avg("is_welcome5").over(w_recent).alias("is_welcome5_avg_2"),
        F.avg("is_welcome5").over(w_ordered).alias("is_welcome5_avg_2_2"),
        F.first("channel", ignorenulls=True).over(w_running).alias("first_known_channel"),
    )
)

metrics = (
    with_windows
    .filter(F.col("is_welcome5_avg_2").between(23, 56))
    .select(
        "*",
        F.last("is_welcome5_avg_2_2", ignorenulls=True).over(w4).alias("last_known_is_welcome5_avg_2_2"),
        F.rank().over(w5).alias("discount_pct_rank"),
        F.ntile(3).over(w6).alias("is_welcome5_avg_2_2_bucket"),
        F.avg("is_welcome5").over(w7).alias("is_welcome5_avg_5_prior"),
    )
)

filtered_promotions = (
    metrics
    .filter(F.col("is_welcome5").between(39, 78))
    .withColumn("least_is_welcome5_avg_5_prior_discount_pct", F.least("is_welcome5_avg_5_prior", "discount_pct"))
)

valid = (
    filtered_promotions
    .filter(F.col("last_known_is_welcome5_avg_2_2").between(41, 75))
    .select(
        "id",
        "ends_on",
        "ends_on_minus_32d",
        "channel_clean",
        "is_welcome5_avg_2",
        "is_welcome5_avg_2_2",
        "last_known_is_welcome5_avg_2_2",
        "discount_pct_rank",
        "is_welcome5_avg_2_2_bucket",
        "is_welcome5_avg_5_prior",
        (F.col("last_known_is_welcome5_avg_2_2") * F.col("is_welcome5_avg_2_2_bucket")).alias("last_known_is_welcome5_avg_2_2_times_is_welcome5_avg_2_2_bucket"),
    )
)

result = (
    valid
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .withColumn("discount_pct_rank_tier", F.when(F.col("discount_pct_rank") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "ends_on"))
    .cube("year_start", "discount_pct_rank_tier", "activity_month")
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("last_known_is_welcome5_avg_2_2"), 2).alias("avg_last_known_is_welcome5_avg_2_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("last_known_is_welcome5_avg_2_2_times_is_welcome5_avg_2_2_bucket").alias("total_last_known_is_welcome5_avg_2_2_times_is_welcome5_avg_2_2_bucket"),
        F.grouping_id().alias("gid"),
    )
)
