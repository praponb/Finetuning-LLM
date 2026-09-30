from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") > 140000)
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.l2_count") >= 4)
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("ds.l2_count"))
    .orderBy(F.col("dept_name").desc(), F.col("l2_count").desc(), "id")
    .limit(20)
)
