from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Designer", "Analyst"]))
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0))) / F.count("*")), 1).alias("pct_intern"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "inner")
    .filter(F.col("department_stats.pct_intern") > 55)
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.dept_name"),
        F.col("department_stats.avg_salary"),
        F.col("department_stats.pct_intern"),
    )
    .orderBy(F.col("location").desc(), "id")
    .limit(5)
)
