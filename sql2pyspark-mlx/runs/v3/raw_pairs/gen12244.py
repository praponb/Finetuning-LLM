from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "outer")
    .filter(~F.col("e.level").isin(["L4", "L2"]))
    .select(
        F.col("e.job_title"),
        F.col("e.hire_date"),
        F.col("e.email"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)
