from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("t.engineer_count"),
        F.col("t.salary_range"),
    )
    .orderBy("engineer_count", "budget", "id")
    .limit(1)
)
