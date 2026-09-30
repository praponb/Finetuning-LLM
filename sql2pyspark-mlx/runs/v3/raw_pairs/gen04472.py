from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.lower(F.col("e.full_name")).like("%aro%") & (F.upper(F.trim(F.col("e.email"))) == "CAROL@COMPANY.CO"))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.level").alias("manager_level"),
        F.col("m.full_name").alias("manager_full_name"),
        F.when(F.col("e.level").isin(["L2", "L5"]), "Priority").otherwise("Other").alias("level_group"),
    )
)
