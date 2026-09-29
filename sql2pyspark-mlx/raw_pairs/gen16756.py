from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").eqNullSafe(F.lit("Tokyo")))
    .withColumn("location_dept_name_key", F.concat_ws("-", "location", "dept_name"))
)

result = (
    cleaned
    .groupBy("location")
    .agg(
        F.percentile_approx("budget", 0.9).alias("budget_p90"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_rows"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
    )
    .orderBy(F.col("engineering_rows").desc())
)
