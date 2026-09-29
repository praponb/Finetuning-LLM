from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.l1_count") >= 20)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ebd.l5_salary"),
        F.col("ebd.l1_count"),
    )
)
