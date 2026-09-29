from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("budget") > 3017000) | ~F.col("dept_name").isin(["Engineering", "Marketing", "HR"]))
)

location_group_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("location").isin(["Chiang Mai", "London"]), "core").otherwise("other").alias("location_group"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
    )
)

result = (
    location_group_summary
    .select("location_group", "stddev_budget", "bangkok_count")
    .orderBy("location_group")
)
