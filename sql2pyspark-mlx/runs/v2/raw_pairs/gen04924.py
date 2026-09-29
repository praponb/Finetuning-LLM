from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("d.dept_name") != "Legal") & (F.col("e.dept_id") == 22))
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("e.email"),
        F.col("e.is_remote"),
    )
    .orderBy(F.col("email").desc())
)
