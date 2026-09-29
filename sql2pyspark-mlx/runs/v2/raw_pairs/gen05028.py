from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(~F.col("e.is_remote"))
    .select(
        F.col("d.dept_name"),
        F.col("e.hire_date"),
        F.col("e.salary"),
        F.col("e.level"),
    )
    .orderBy(F.col("level").desc())
)
