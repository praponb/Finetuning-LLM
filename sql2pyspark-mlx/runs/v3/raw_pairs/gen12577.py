from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.dept_id").isin([3, 7, 20, 40]) | F.month(F.col("m.hire_date")).isin([2, 3, 5]))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy("manager_full_name")
)
