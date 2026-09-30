from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "left")
    .filter(F.col("D.pct_l5") > 77)
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.location"), F.col("D.pct_l5"))
    .orderBy(F.col("id").desc(), F.col("location").desc())
)
