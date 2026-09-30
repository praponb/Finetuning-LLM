from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.hire_date").between("2024-09-02", "2024-11-01"))
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
        F.col("e.job_title"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
    .orderBy(F.col("id").desc())
)
