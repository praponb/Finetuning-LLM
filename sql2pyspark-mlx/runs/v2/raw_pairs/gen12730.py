from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("job_title") != "Director")
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.pct_l3") >= 68)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("dt.pct_l3"),
        F.col("dt.last_hire_date"),
    )
    .orderBy("last_hire_date")
)
