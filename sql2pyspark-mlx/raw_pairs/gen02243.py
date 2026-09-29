from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("salary") > 138000)
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "left")
    .filter(F.col("D.l2_count") >= 7)
    .select(F.col("d.dept_name"), F.col("D.l2_count"))
    .orderBy("dept_name")
)
