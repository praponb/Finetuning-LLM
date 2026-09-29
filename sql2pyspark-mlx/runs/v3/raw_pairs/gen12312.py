from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("manager_id") == 33)
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ds.total_salary"),
    )
    .orderBy(F.col("id").desc(), F.col("budget").desc())
)
