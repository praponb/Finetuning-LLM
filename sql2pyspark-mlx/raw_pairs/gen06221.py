from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.budget") < 3098000)
    .select(
        F.col("e.level"),
        F.col("e.manager_id"),
        F.col("e.is_remote"),
        F.col("d.budget"),
    )
    .orderBy(F.col("manager_id").desc(), "budget")
)
