from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("salary") <= 58000)
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("dt.last_hire_date"),
        F.col("dt.l1_count"),
    )
)
