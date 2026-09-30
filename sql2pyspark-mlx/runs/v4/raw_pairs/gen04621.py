from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.budget") >= 3424000)
    .select(
        F.col("d.dept_name"),
        F.col("e.job_title"),
        F.when(F.col("d.budget") >= 4125000, "High").when(F.col("d.budget") >= 1402000, "Medium").otherwise("Low").alias("budget_tier"),
    )
    .orderBy("dept_name")
)
