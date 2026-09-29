from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.salary") >= 122000)
    .select(
        F.col("d.location"),
        F.col("e.full_name"),
        F.col("e.salary"),
        F.col("e.dept_id"),
    )
)
