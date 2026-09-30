from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w_recent = Window.partitionBy("location")
w_part = Window.partitionBy("dept_name")
w_part_2 = Window.partitionBy("location", "dept_name")

valid = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Sales", "Marketing"]))
    .select(
        "*",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
        F.coalesce("location", F.lit("unassigned")).alias("location_filled_2"),
        F.coalesce("location", F.lit("OTHER")).alias("resolved_location"),
    )
)

kept_keys = (
    valid
    .select("location")
    .intersectAll(
        df_departments
        .filter(~F.col("dept_name").isin(["Finance", "HR"]))
        .select("location")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("budget_percent_rank", F.percent_rank().over(w))
    .withColumn("location_max_budget", F.max("budget").over(w_recent))
    .withColumn("location_avg_budget", F.avg("budget").over(w_recent))
    .withColumn("dept_name_count_budget", F.count("budget").over(w_part))
    .withColumn("location_stddev_samp_budget", F.stddev_samp("budget").over(w_part_2))
)

eligible = (
    scored
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([4, 5, 37])), "left_semi")
)

result = (
    eligible
    .groupBy(
        "location",
        F.when(F.col("budget_percent_rank") >= 0.77, "HIGH").otherwise("LOW").alias("budget_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("location_stddev_samp_budget", [0.25, 0.5, 0.75]).alias("location_stddev_samp_budget_quartiles"),
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("location_max_budget")).otherwise(0)).alias("marketing_location_max_budget"),
        F.percentile_approx("dept_name_count_budget", 0.9).alias("dept_name_count_budget_p90"),
        F.round(F.avg("location_stddev_samp_budget"), 2).alias("avg_location_stddev_samp_budget"),
        F.avg("location_stddev_samp_budget").alias("avg_location_stddev_samp_budget_2"),
        F.percentile_approx("dept_name_count_budget", [0.25, 0.5, 0.75]).alias("dept_name_count_budget_quartiles"),
    )
    .filter(F.col("marketing_location_max_budget") > 0)
    .orderBy(F.col("avg_location_stddev_samp_budget").desc())
)
