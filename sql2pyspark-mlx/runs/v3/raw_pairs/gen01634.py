from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.col("is_remote"))

summary = (
    filtered_employees
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    summary
    .select("dept_id", "job_title_values", "min_salary")
    .orderBy("job_title_values", "dept_id")
    .limit(10)
)
