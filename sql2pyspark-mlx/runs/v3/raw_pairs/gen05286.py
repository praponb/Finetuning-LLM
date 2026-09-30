from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.total_count") > 6)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ebd.total_count"),
        F.col("ebd.first_hire_date"),
    )
    .orderBy(F.col("id").desc())
)
