from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.budget").between(1289000, 2585000))
    .select(
        F.col("e.dept_id"),
        F.col("e.email"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
    .orderBy("dept_name")
)
