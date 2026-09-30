from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.round(F.col("e.salary"), 2).alias("salary_rounded"),
    )
    .orderBy(F.col("manager_dept_id").desc(), "id")
    .limit(20)
)
