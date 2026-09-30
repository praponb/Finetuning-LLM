from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.dept_id").isin([4, 23, 27, 35]) & ~F.col("e.is_remote"))
    .select(
        F.col("e.job_title"),
        F.col("e.level"),
        F.col("e.manager_id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.datediff(F.current_date(), F.col("e.hire_date")).alias("days_since_hire_date"),
    )
)
