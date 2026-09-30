from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("full_name").like("%Pim%"))
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(F.col("d.budget"), F.col("ebd.l3_count"), F.col("ebd.l4_count"))
    .orderBy(F.col("budget").desc())
)
