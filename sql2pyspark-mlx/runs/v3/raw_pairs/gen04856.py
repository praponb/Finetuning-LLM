from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("level").isin(["L2", "L5", "L4"]))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ebd.intern_count"),
        F.col("ebd.cnt"),
    )
)
