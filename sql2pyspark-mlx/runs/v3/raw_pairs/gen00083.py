from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["Berlin", "Singapore", "Tokyo"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.col("e.level") == "L3")
    .select(
        F.col("e.id"),
        F.col("e.salary"),
        F.col("e.manager_id"),
        F.col("e.is_remote"),
    )
    .orderBy("manager_id", "id")
)
