from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") > 141000)
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.pct_l3") > 71)
    .select(F.col("d.location"), F.col("ds.pct_l3"), F.col("ds.unique_depts"))
    .orderBy("pct_l3")
)
