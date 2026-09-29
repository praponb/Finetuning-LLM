from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("full_name").like("bob%"))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "left")
    .filter(F.col("E.l3_count") > 4)
    .select(F.col("d.location"), F.col("E.l1_count"), F.col("E.l3_count"))
    .orderBy("location")
)
