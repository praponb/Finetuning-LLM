from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary")).otherwise(0)).alias("intern_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.intern_salary") > 76)
    .select(F.col("d.dept_name"), F.col("ebd.total_salary"), F.col("ebd.intern_salary"))
)
