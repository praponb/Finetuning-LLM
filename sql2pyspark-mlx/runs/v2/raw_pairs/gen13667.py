from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0))) / F.count("*")), 1).alias("pct_intern"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(F.col("d.budget"), F.col("ebd.pct_intern"), F.col("ebd.avg_salary"))
    .orderBy("budget")
)
