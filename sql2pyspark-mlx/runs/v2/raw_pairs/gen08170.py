from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.manager_id") == 28)
    .select(
        F.col("e.id"),
        F.col("e.salary"),
        F.col("e.email"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("d.budget"),
    )
    .orderBy(F.col("id").desc(), "salary")
)
