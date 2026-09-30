from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.job_title").isin(["Analyst", "Intern"]))
    .select(
        F.col("d.location"),
        F.col("e.full_name"),
        F.col("e.email"),
        F.col("e.level"),
    )
)
