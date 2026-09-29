from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("monthly_fee_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy(F.col("first_known_monthly_fee").desc_nulls_first(), "id")
w_part = Window.partitionBy("plan").orderBy("running_avg_days_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.year("end_date"))

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-10-05").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

scored = (
    valid
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "days_start_date_to_end_date",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_5"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
    )
)

metrics = (
    scored
    .withColumn("running_max_days_start_date_to_end_date", F.max("days_start_date_to_end_date").over(w3))
    .withColumn("running_avg_days_start_date_to_end_date", F.avg("days_start_date_to_end_date").over(w3))
    .withColumn("running_sum_days_start_date_to_end_date", F.sum("days_start_date_to_end_date").over(w3))
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

scored_subscriptions = (
    ranked
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "days_start_date_to_end_date",
        "monthly_fee_avg_5",
        "running_avg_days_start_date_to_end_date",
        F.sum("running_max_days_start_date_to_end_date").over(w_part).alias("running_sum_running_max_days_start_date_to_end_date"),
        F.min("running_max_days_start_date_to_end_date").over(w_part).alias("running_min_running_max_days_start_date_to_end_date"),
        F.count("running_max_days_start_date_to_end_date").over(w_part).alias("running_count_running_max_days_start_date_to_end_date"),
        F.min("monthly_fee_avg_5").over(w_recent).alias("end_date_year_min_monthly_fee_avg_5"),
    )
)

result = (
    scored_subscriptions
    .withColumn("week_start", F.date_trunc("WEEK", "start_date"))
    .rollup("week_start")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("running_avg_days_start_date_to_end_date")).otherwise(0)).alias("premium_running_avg_days_start_date_to_end_date"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
        F.max(F.when(F.col("plan") == "standard", F.col("monthly_fee_avg_5"))).alias("standard_max_monthly_fee_avg_5"),
        F.percentile_approx("end_date_year_min_monthly_fee_avg_5", [0.25, 0.5, 0.75]).alias("end_date_year_min_monthly_fee_avg_5_quartiles"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.grouping("week_start").alias("gid"),
    )
)
