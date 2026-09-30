from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2025-01-10")
    .groupBy("is_remote", "level", "dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.avg("salary").alias("avg_salary"),
    )
)
