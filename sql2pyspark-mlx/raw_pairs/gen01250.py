from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.avg_salary") >= 64)
    .select(F.col("departments.budget"), F.col("t.avg_salary"), F.col("t.l3_count"))
    .orderBy("avg_salary")
)
