from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") >= 95000)
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.pct_l3") >= 48)
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("d.location"), F.col("t.pct_l3"))
    .orderBy("dept_name")
)
