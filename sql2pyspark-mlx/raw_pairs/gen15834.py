from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.salary").between(70000, 76000))
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("e.manager_id"),
        F.when(F.col("e.job_title") == "Designer", 1).otherwise(0).alias("is_designer"),
    )
    .orderBy(F.col("is_designer").desc(), F.col("manager_id").desc())
)
