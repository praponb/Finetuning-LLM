from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("e.manager_id") == 2) & F.col("d.budget").between(1329000, 2391000))
    .select(
        F.col("e.id"),
        F.col("e.manager_id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
    )
    .orderBy(F.col("manager_id").desc(), "location")
)
