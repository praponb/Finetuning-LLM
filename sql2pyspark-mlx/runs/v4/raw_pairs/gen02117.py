from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.month("hire_date").isin([1, 7, 12]))
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.pct_l4") > 45)
    .select(F.col("d.location"), F.col("dt.pct_l4"))
    .orderBy(F.col("pct_l4").desc(), F.col("location").desc())
)
