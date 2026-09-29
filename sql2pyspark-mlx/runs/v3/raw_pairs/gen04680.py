from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.level") == "L2")
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.date_format(F.col("e.hire_date"), "MMM yyyy").alias("hire_date_label"),
    )
)
