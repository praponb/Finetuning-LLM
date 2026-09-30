from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.dept_id").isin([18, 31, 32, 36]))
    .select(
        F.col("emp.id"),
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.when(F.col("mgr.is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy("manager_full_name", F.col("is_remote_label").desc())
)
