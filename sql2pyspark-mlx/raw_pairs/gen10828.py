from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.l4_count") > 44)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("t.manager_count"),
        F.col("t.l4_count"),
    )
    .orderBy("dept_name", F.col("l4_count").desc())
)
