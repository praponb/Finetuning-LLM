from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.cnt") > 4)
    .select(
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("t.l2_count"),
        F.col("t.cnt"),
    )
)
