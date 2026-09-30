from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.location").isin(["London", "Tokyo", "Chiang Mai"]) & F.col("e.dept_id").isin([4, 28, 39]))
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("e.hire_date"),
    )
)
