from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("days_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("days_start_date_to_end_date").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("cumulative_days_start_date_to_end_date").desc_nulls_last())

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.lower("plan").alias("plan_lower"),
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.sum("days_start_date_to_end_date").over(w).alias("cumulative_days_start_date_to_end_date"),
        F.percent_rank().over(w2).alias("days_start_date_to_end_date_percent_rank"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "plan_lower",
        "days_start_date_to_end_date_percent_rank",
    )
)

base = (
    latest
    .filter(F.col("start_date") > F.lit("2023-03-13").cast("date"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "plan_lower",
        F.coalesce("days_start_date_to_end_date_percent_rank", F.lit(0)).alias("days_start_date_to_end_date_percent_rank_filled"),
        F.upper("plan").alias("plan_upper"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("YEAR", "start_date").alias("year_start"), "plan")
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.count("*").alias("cnt"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
