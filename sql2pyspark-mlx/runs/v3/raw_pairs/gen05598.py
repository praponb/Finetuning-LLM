from pyspark.sql import functions as F

recent_employees = df_employees.filter((F.col("salary") < 59000) & F.col("dept_id").isin([6, 17, 39]))

hire_month_summary = (
    recent_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = hire_month_summary
