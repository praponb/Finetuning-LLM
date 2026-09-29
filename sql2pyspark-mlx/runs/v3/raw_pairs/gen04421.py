from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ds.l1_count"),
        F.col("ds.min_salary"),
    )
    .orderBy(F.col("l1_count").desc())
)
