from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(F.col("d.location"), F.col("ebd.salary_range"), F.col("ebd.job_title_list"))
)
