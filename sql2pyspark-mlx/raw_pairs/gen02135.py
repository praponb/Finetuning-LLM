from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.budget").between(1299000, 1904000) & (F.col("d.location") == "Bangkok"))
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("e.job_title"),
        F.col("e.hire_date"),
        F.upper(F.col("e.full_name")).alias("full_name_upper"),
    )
)
