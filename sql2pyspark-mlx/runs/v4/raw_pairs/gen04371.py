from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.l4_salary") >= 66)
    .select(F.col("d.id"), F.col("d.location"), F.col("ebd.l4_salary"))
    .orderBy(F.col("location").desc())
)
