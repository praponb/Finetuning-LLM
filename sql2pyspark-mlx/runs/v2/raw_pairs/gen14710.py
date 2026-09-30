from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(
        F.when(F.col("d.budget") >= 3700000, "high").when(F.col("d.budget") >= 1256000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.job_title") == "Intern", 1).otherwise(0))) / F.count("*")), 1).alias("pct_intern"),
        F.count("*").alias("num_employees"),
    )
    .orderBy(F.col("num_employees").desc(), "budget_bucket")
)
