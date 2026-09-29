from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("weeks_start_date_to_end_date").desc(), "id")
w3 = Window.orderBy("weeks_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.email"))
)

with_windows = (
    joined
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("running_sum_monthly_fee"),
    )
)

filtered = (
    with_windows
    .filter(F.col("end_date") > F.lit("2023-06-20").cast("date"))
    .select(
        "id",
        "start_date",
        "end_date",
        (F.datediff("end_date", "start_date") / 7).alias("weeks_start_date_to_end_date"),
    )
)

metrics = (
    filtered
    .withColumn("weeks_start_date_to_end_date_bucket", F.ntile(10).over(w2))
    .withColumn("last_known_weeks_start_date_to_end_date", F.last("weeks_start_date_to_end_date", ignorenulls=True).over(w3))
)

filtered_subscriptions = (
    metrics
    .filter((F.col("start_date") > F.to_date(F.lit("2023-09-11"))) & (F.col("last_known_weeks_start_date_to_end_date") > 44))
    .withColumn("weeks_start_date_to_end_date_rounded", F.round("weeks_start_date_to_end_date", 1))
    .withColumn("weeks_start_date_to_end_date_whole", F.ceil("weeks_start_date_to_end_date"))
)

result = (
    filtered_subscriptions
    .groupBy(F.date_trunc("MONTH", "start_date").alias("activity_month"))
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("weeks_start_date_to_end_date", 0.95).alias("weeks_start_date_to_end_date_p95"),
        F.percentile_approx("weeks_start_date_to_end_date_rounded", [0.25, 0.5, 0.75]).alias("weeks_start_date_to_end_date_rounded_quartiles"),
        F.percentile_approx("weeks_start_date_to_end_date_whole", 0.9).alias("weeks_start_date_to_end_date_whole_p90"),
    )
    .filter(F.col("num_rows") < 1000)
    .orderBy(F.col("activity_month").desc())
)
