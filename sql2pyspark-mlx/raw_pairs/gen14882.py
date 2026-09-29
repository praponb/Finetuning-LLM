from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("dt.l4_count"), F.col("dt.max_salary"))
)
