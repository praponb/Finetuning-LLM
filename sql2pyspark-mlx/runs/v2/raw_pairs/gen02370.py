from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("e.job_title") != "Manager") | F.col("e.salary").between(80000, 129000))
    .select(
        F.col("e.manager_id"),
        F.col("e.dept_id"),
        F.col("e.level"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
    .orderBy("level")
)
