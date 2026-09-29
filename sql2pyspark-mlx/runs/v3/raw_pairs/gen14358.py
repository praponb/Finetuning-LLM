from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        F.col("d.location"),
        F.col("e.job_title"),
        F.col("e.hire_date"),
        F.col("e.dept_id"),
        F.when(F.col("d.budget") >= 4045000, "High").when(F.col("d.budget") >= 1468000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)
