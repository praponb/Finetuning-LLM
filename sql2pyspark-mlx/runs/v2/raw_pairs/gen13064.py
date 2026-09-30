from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter((F.col("d.budget") > 2018000) & F.month(F.col("e.hire_date")).isin([5, 10, 11]))
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
        F.col("e.email"),
    )
)
