from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("dept_id").isin([18, 21, 27, 30]))
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("num_employees"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.num_employees") >= 4)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ebd.num_employees"),
        F.col("ebd.intern_count"),
    )
)
