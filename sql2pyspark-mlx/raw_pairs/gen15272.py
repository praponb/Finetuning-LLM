from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("location").isin(["London", "Chiang Mai"]))

location_summary = (
    recent_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3989000, "high").when(F.col("budget") >= 1895000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.countDistinct("budget").alias("unique_locations"),
    )
)

result = (
    location_summary
    .filter(F.col("unique_locations") > 8)
    .select("location", "budget_bucket", "dept_name_values")
    .orderBy("dept_name_values", F.col("location").desc())
)
