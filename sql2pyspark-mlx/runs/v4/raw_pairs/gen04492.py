from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") > "2025-02-01") | ~F.col("job_title").isin(["Analyst", "Director"]))
    .groupBy("level", "is_remote", "job_title")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .orderBy("job_title", "level", "is_remote")
    .limit(10)
)
