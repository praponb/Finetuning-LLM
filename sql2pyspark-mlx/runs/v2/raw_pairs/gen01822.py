from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .select(
        F.col("e.email"),
        F.col("e.manager_id"),
        F.col("e.is_remote"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.round((F.col("d.budget") * 1.15), 2).alias("adjusted_budget"),
    )
    .orderBy(F.col("manager_id").desc())
)
