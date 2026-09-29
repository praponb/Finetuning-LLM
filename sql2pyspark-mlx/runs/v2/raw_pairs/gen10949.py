from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.l2_salary") > 38)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.l2_salary"),
    )
)
