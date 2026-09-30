from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.year("end_date")).orderBy(F.col("days_since_end_date").desc())
w3 = Window.partitionBy("plan").orderBy("days_since_end_date", "id").rowsBetween(-3, -1)
w4 = Window.orderBy(F.col("days_since_end_date_rank").asc_nulls_first())
w5 = Window.orderBy(F.col("days_since_end_date_rank").desc())

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-07-29").cast("date")) & (F.col("customer_id") == 5))
    .select(
        "*",
        F.trim("plan").alias("clean_plan"),
        F.length("status").alias("status_length"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

base_subscriptions = (
    latest
    .filter((F.col("start_date") > F.lit("2023-04-23").cast("date")) & F.col("monthly_fee").between(110, 230))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "clean_plan",
        "status_length",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)

scored = (
    base_subscriptions
    .select(
        "id",
        "start_date",
        "end_date",
        "clean_plan",
        F.rank().over(w2).alias("days_since_end_date_rank"),
        F.avg("days_since_end_date").over(w3).alias("days_since_end_date_avg_3_prior"),
    )
)

matched_keys = (
    scored
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("start_date").between("2024-01-01", "2024-07-19"))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "start_date",
        "clean_plan",
        "days_since_end_date_rank",
        "days_since_end_date_avg_3_prior",
        F.percent_rank().over(w4).alias("days_since_end_date_rank_percent_rank"),
        F.cume_dist().over(w5).alias("days_since_end_date_rank_cume_dist"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("YEAR", "start_date").alias("year_start"))
    .agg(
        F.percentile_approx("days_since_end_date_rank_percent_rank", 0.9).alias("days_since_end_date_rank_percent_rank_p90"),
        F.max("days_since_end_date_rank").alias("max_days_since_end_date_rank"),
        F.percentile_approx("days_since_end_date_rank", 0.9).alias("days_since_end_date_rank_p90"),
    )
    .filter(F.col("days_since_end_date_rank_percent_rank_p90") > 0)
    .orderBy(F.col("year_start").asc_nulls_last())
)
