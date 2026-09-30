from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.hire_date") <= "2024-01-01")
    .select(
        F.col("e.id"),
        F.col("e.job_title"),
        F.col("d.budget"),
        F.when(F.col("e.job_title") == "Designer", 1).otherwise(0).alias("is_designer"),
    )
)
