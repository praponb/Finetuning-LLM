from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.dept_id").isin([16, 21, 30, 34]))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.salary").alias("manager_salary"),
        F.when(F.col("m.is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
