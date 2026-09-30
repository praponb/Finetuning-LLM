from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() & (F.col("is_remote") | (F.col("dept_id") == 10)))
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("hire_date").alias("latest_hire_date"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
