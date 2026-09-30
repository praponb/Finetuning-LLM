from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.min("hire_date").alias("min_hire_date"),
    )
)
