from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.manager_id").isNull())
    .select(
        F.col("d.location"),
        F.col("e.salary"),
        F.col("e.hire_date"),
        F.col("e.full_name"),
        F.when(F.col("d.budget") >= 4308000, "High").when(F.col("d.budget") >= 896000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)
