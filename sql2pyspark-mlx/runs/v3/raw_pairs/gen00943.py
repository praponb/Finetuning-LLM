from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.level") == "L2")
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("e.job_title"),
        F.col("e.dept_id"),
    )
)
