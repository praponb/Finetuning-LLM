from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("hire_date") > "2025-09-05")
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.min_salary") > 134000)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("ebd.min_salary"),
        F.col("ebd.job_title_values"),
    )
    .orderBy(F.col("dept_name").desc())
)
