from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy(F.col("running_min_budget").desc(), "id")
w3 = Window.partitionBy("location").orderBy("running_min_budget", "id")
w4 = Window.orderBy("running_min_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_max_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_departments
    .select(
        "id",
        "location",
        F.min("budget").over(w).alias("running_min_budget"),
        F.sum("budget").over(w).alias("running_sum_budget"),
        F.avg("budget").over(w).alias("running_avg_budget"),
        F.max("budget").over(w).alias("running_max_budget"),
        F.sum("budget").over(w).alias("cumulative_budget"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    deduped
    .filter(F.col("cumulative_budget") != 12009758)
    .withColumn("location_length", F.length("location"))
    .withColumn("running_max_budget_per_running_avg_budget", F.round((F.col("running_max_budget") / F.col("running_avg_budget")), 2))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "running_min_budget",
        "running_max_budget",
        "location_length",
        "running_max_budget_per_running_avg_budget",
    )
)

scored = (
    ranked
    .filter(F.col("running_min_budget") >= 3687000)
    .withColumn("last_known_running_max_budget_per_running_avg_budget", F.last("running_max_budget_per_running_avg_budget", ignorenulls=True).over(w4))
    .withColumn("first_known_location_length", F.first("location_length", ignorenulls=True).over(w5))
)

cleaned = (
    scored
    .filter(F.col("running_min_budget") >= 2233000)
    .select(
        "id",
        "running_max_budget",
        "location_length",
        "last_known_running_max_budget_per_running_avg_budget",
        (F.col("running_max_budget_per_running_avg_budget") - F.col("last_known_running_max_budget_per_running_avg_budget")).alias("running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget"),
        (F.col("running_max_budget_per_running_avg_budget") - F.col("running_max_budget")).alias("running_max_budget_per_running_avg_budget_minus_running_max_budget"),
    )
)

result = (
    cleaned
    .withColumn("running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget_tier", F.when(F.col("running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget") >= 78, "PREMIUM").otherwise("STANDARD"))
    .withColumn("location_length_tier", F.when(F.col("location_length") >= 64, "HIGH").otherwise("LOW"))
    .withColumn("running_max_budget_per_running_avg_budget_minus_running_max_budget_tier", F.when(F.col("running_max_budget_per_running_avg_budget_minus_running_max_budget") >= 63, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget_tier", "location_length_tier", "running_max_budget_per_running_avg_budget_minus_running_max_budget_tier"], ["running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget_tier", "location_length_tier"], ["running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget_tier"], []],
        "running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget_tier",
        "location_length_tier",
        "running_max_budget_per_running_avg_budget_minus_running_max_budget_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("running_max_budget", [0.25, 0.5, 0.75]).alias("running_max_budget_quartiles"),
        F.min("running_max_budget").alias("min_running_max_budget"),
        F.count("*").alias("row_count_2"),
    )
)
