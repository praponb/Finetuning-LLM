from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.col("dept_id").isin([9, 10, 19]))

job_title_summary = (
    filtered_employees
    .groupBy("job_title", F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)

result = job_title_summary
