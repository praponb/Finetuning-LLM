from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0))) / F.count("*")), 1).alias("pct_intern"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.pct_intern") > 34)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.first_hire_date"),
        F.col("ds.pct_intern"),
    )
    .orderBy("first_hire_date")
)
