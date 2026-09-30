from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .groupBy(F.col("departments.location"))
    .agg(
        F.max(F.col("employees.hire_date")).alias("last_hire_date"),
        F.sum(F.when(F.col("employees.level") == "L2", F.col("employees.salary")).otherwise(0)).alias("l2_salary"),
    )
    .orderBy("last_hire_date", "location")
    .limit(10)
)
