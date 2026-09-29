from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location")
w3 = Window.orderBy("budget_bucket", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w5 = Window.orderBy(F.col("budget_bucket").desc(), "id")
w6 = Window.partitionBy("location").orderBy("event_seq", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("location").orderBy("location_stddev_samp_budget", "id")
w_hist = Window.partitionBy("location").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("clean_location", F.trim("location"))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

with_windows = (
    cleaned
    .select(
        "id",
        "location",
        "budget",
        "clean_location",
        F.ntile(4).over(w).alias("budget_bucket"),
        F.stddev_samp("budget").over(w2).alias("location_stddev_samp_budget"),
    )
)

with_windows_departments = (
    with_windows
    .select(
        "id",
        "location",
        "budget",
        "clean_location",
        "budget_bucket",
        "location_stddev_samp_budget",
        F.avg("location_stddev_samp_budget").over(w3).alias("location_stddev_samp_budget_avg_3"),
        F.ntile(10).over(w4).alias("budget_bucket_2"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

metrics = (
    with_windows_departments
    .withColumn("event_seq_avg_5", F.avg("event_seq").over(w6))
    .withColumn("next_location_stddev_samp_budget", F.lead("location_stddev_samp_budget", 1).over(w7))
)

enriched = (
    metrics
    .filter(F.col("location_stddev_samp_budget").between(3056048, 3507204))
    .withColumn("running_min_location_stddev_samp_budget", F.min("location_stddev_samp_budget").over(w_hist))
    .withColumn("running_count_location_stddev_samp_budget", F.count("location_stddev_samp_budget").over(w_hist))
    .withColumn("running_max_location_stddev_samp_budget", F.max("location_stddev_samp_budget").over(w_hist))
)

result = (
    enriched
    .select(
        "id",
        "budget",
        "clean_location",
        "budget_bucket",
        "location_stddev_samp_budget",
        "location_stddev_samp_budget_avg_3",
        "event_seq",
        "event_seq_avg_5",
        "running_count_location_stddev_samp_budget",
    )
    .orderBy(F.col("running_count_location_stddev_samp_budget").asc_nulls_last())
)
