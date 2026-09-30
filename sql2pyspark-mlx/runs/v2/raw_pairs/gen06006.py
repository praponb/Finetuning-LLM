from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.lower(F.trim("email")) == "ivy@gmail.com")
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.id"), F.col("d.budget"), F.col("t.l5_count"), F.col("t.l2_count"))
    .orderBy("budget")
)
