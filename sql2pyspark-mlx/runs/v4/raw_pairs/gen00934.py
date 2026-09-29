from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("hire_date") < "2024-11-07")
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("t.l3_salary"),
        F.col("t.stddev_salary"),
    )
    .orderBy("l3_salary")
)
