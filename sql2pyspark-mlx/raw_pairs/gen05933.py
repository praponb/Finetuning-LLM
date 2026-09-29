from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w3 = Window.partitionBy("dept_name")
w4 = Window.orderBy("budget", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("running_budget_tier").orderBy(F.col("hr_count").desc())

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_upper", F.upper("dept_name"))
)

with_windows = (
    base
    .select(
        "*",
        F.stddev_samp("budget").over(w).alias("location_stddev_samp_budget"),
        F.cume_dist().over(w2).alias("budget_cume_dist"),
        F.max("budget").over(w3).alias("dept_name_max_budget"),
        F.avg("budget").over(w4).alias("budget_avg_2_prior"),
        F.sum("budget").over(w5).alias("running_budget"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_filled", F.coalesce("location", F.lit("Unknown")))
)

valid = (
    cleaned
    .filter(F.col("location").isNotNull())
    .select("*", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("running_budget") >= 21274066, "HIGH").otherwise("LOW").alias("running_budget_tier"),
        F.when(F.col("budget_avg_2_prior") >= 3757273, "HIGH").otherwise("LOW").alias("budget_avg_2_prior_tier"),
        F.when(F.col("location_stddev_samp_budget") >= 3557793, "PREMIUM").otherwise("STANDARD").alias("location_stddev_samp_budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.percentile_approx("budget_filled", [0.25, 0.5, 0.75]).alias("budget_filled_quartiles"),
    )
    .withColumn("hr_count_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("location_stddev_samp_budget_tier").desc_nulls_first())
)
