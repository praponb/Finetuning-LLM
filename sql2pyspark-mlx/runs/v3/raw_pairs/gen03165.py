from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.length("email") >= 13)
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.l3_count"),
        F.col("ds.last_hire_date"),
    )
    .orderBy("budget")
)
