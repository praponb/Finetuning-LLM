from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("d.budget") >= 1162000) & (F.col("e.level") == "L4"))
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("e.level"),
        F.col("e.dept_id"),
        F.col("e.hire_date"),
    )
    .orderBy("dept_name")
)
