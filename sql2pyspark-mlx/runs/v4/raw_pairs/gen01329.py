from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.max(F.col("e.hire_date")).alias("latest_hire_date"),
        F.count(F.when(F.col("e.level") == "L2", 1)).alias("l2_count"),
    )
    .orderBy("latest_hire_date")
)
