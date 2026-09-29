from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.l1_count") >= 5)
    .select(F.col("departments.budget"), F.col("t.avg_salary"), F.col("t.l1_count"))
    .orderBy(F.col("l1_count").desc())
)
