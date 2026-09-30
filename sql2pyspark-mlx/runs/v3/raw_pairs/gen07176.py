from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter((F.col("d.budget") > 1101000) & (F.col("e.dept_id") == 18))
    .select(
        F.col("e.id"),
        F.col("e.email"),
        F.col("e.job_title"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
    .orderBy("email", F.col("budget").desc())
)
