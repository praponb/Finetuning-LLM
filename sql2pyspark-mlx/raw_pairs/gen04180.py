from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.l5_count") > 42)
    .select(F.col("d.budget"), F.col("ebd.l5_count"), F.col("ebd.last_hire_date"))
)
