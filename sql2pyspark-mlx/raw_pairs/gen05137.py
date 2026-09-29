from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("level").isin(["L3", "L1"]))
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(F.col("d.budget"), F.col("ebd.stddev_salary"), F.col("ebd.avg_salary"))
    .orderBy(F.col("avg_salary").desc())
)
