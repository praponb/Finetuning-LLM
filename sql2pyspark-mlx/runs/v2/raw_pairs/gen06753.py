from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.dept_name").isin(["HR", "Sales", "Marketing"]))
    .select(
        F.col("e.salary"),
        F.col("e.job_title"),
        F.col("e.level"),
        F.col("d.dept_name"),
    )
)
