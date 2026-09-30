from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.dept_id"),
        F.col("e.salary"),
        F.col("e.manager_id"),
        F.col("d.budget"),
        F.col("d.location"),
    )
    .orderBy("manager_id")
)
