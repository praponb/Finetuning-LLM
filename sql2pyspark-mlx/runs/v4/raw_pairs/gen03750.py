from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.l5_count") > 24)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("t.first_hire_date"),
        F.col("t.l5_count"),
    )
    .orderBy("dept_name")
)
