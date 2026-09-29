from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("full_name").like("%Car%"))
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.l1_count") > 6)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.l1_count"),
        F.col("t.l4_count"),
    )
)
