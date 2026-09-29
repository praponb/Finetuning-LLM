from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("dept_name") != "Marketing")
    .withColumn("location_dept_name_key", F.concat_ws("-", "location", "dept_name"))
)

summary = (
    src
    .groupBy("dept_name")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
    )
)

result = (
    summary
    .filter(F.col("berlin_count") > 37)
    .select("dept_name", "avg_budget", "berlin_count")
    .orderBy(F.col("avg_budget").desc())
)
