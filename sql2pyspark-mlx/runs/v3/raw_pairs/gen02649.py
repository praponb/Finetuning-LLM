from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.l3_count") >= 43)
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.latest_hire_date"),
        F.col("ds.l3_count"),
    )
)
