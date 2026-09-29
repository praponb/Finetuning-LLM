from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") >= 140000)
    .groupBy("dept_id")
    .agg(F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.l3_count") >= 39)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ds.l3_count"),
    )
    .orderBy("budget")
)
