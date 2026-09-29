from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ds.total_salary"),
        F.col("ds.l1_count"),
    )
    .orderBy(F.col("l1_count").desc(), "id")
    .limit(5)
)
