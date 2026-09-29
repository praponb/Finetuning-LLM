from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.job_title").isin(["Designer", "Intern"]) & (F.col("e.manager_id") == 1))
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.is_remote"),
        F.col("e.salary"),
        F.col("e.dept_id"),
    )
    .orderBy(F.col("dept_id").desc())
)
