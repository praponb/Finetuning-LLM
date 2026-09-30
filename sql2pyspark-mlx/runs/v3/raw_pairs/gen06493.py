from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.salary").between(114000, 130000) & F.lower(F.col("e.full_name")).like("%lic%"))
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.salary").alias("manager_salary"),
        F.initcap(F.trim(F.col("m.level"))).alias("level_title"),
    )
    .orderBy("employee_dept_id")
)
