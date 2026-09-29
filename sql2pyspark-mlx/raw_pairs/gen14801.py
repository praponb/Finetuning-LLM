from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.length("email") > 12)
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.max_salary") > 86000)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("t.avg_salary"),
        F.col("t.max_salary"),
    )
    .orderBy("location")
)
