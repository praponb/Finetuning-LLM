from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(~F.col("job_title").isin(["Designer", "Manager", "Intern"]))
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("t.unique_depts"),
        F.col("t.pct_manager"),
    )
)
