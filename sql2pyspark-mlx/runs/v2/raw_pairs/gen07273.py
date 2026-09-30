from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "outer")
    .filter(F.col("e.salary") <= 130000)
    .groupBy(F.col("d.dept_name"), F.col("d.location"))
    .agg(
        F.sum(F.when(F.col("e.level") == "L1", 1).otherwise(0)).alias("l1_count"),
        F.max(F.col("e.hire_date")).alias("last_hire_date"),
    )
    .orderBy(F.col("location").desc())
)
