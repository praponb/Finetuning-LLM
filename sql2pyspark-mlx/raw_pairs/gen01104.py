from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("location").orderBy(F.col("budget_avg_5_prior").asc_nulls_last())
w_ordered = Window.partitionBy("location").orderBy("budget_avg_5_prior", "id")
w5 = Window.partitionBy("location").orderBy(F.col("avg_budget_avg_5_prior_cume_dist").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Finance"]))
    .withColumn("location_title", F.initcap(F.trim("location")))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

eligible = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").like("%bob%")), "left_anti")
)

metrics = (
    eligible
    .filter(F.col("budget") <= 1497000)
    .select(
        "id",
        "location",
        "location_title",
        F.percent_rank().over(w).alias("budget_percent_rank"),
        F.avg("budget").over(w2).alias("budget_avg_5_prior"),
    )
)

base = (
    metrics
    .filter(F.col("location").isin(["Singapore", "Tokyo", "Berlin"]))
    .select(
        "id",
        "location",
        "budget_percent_rank",
        "budget_avg_5_prior",
        F.trim("location").alias("clean_location"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled"),
    )
)

metrics_departments = (
    base
    .select(
        "id",
        "location",
        "clean_location",
        F.cume_dist().over(w3).alias("budget_avg_5_prior_cume_dist"),
        F.ntile(10).over(w_ordered).alias("budget_avg_5_prior_bucket"),
    )
)

cleaned = (
    metrics_departments
    .filter(F.col("location").isNotNull())
    .withColumn("clean_location_2", F.trim("location"))
)

result = (
    cleaned
    .groupBy(
        "location",
        F.when(F.col("budget_avg_5_prior_cume_dist") >= 0.66, "HIGH").otherwise("LOW").alias("budget_avg_5_prior_cume_dist_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget_avg_5_prior_cume_dist")).otherwise(0)).alias("bangkok_budget_avg_5_prior_cume_dist"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.round(F.avg("budget_avg_5_prior_bucket"), 2).alias("avg_budget_avg_5_prior_bucket"),
        F.avg("budget_avg_5_prior_cume_dist").alias("avg_budget_avg_5_prior_cume_dist"),
        F.round(F.sum("budget_avg_5_prior_cume_dist") / F.nullif(F.count(F.when(F.col("location") == "London", 1)), F.lit(0)), 2).alias("budget_avg_5_prior_cume_dist_per_london"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
    )
    .filter(F.col("avg_budget_avg_5_prior_cume_dist") > 0)
    .withColumn("avg_budget_avg_5_prior_cume_dist_percent_rank", F.percent_rank().over(w5))
)
