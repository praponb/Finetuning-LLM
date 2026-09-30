from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("num_employees"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.manager_count") >= 4)
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ebd.num_employees"),
        F.col("ebd.manager_count"),
    )
    .orderBy(F.col("manager_count").desc())
)
