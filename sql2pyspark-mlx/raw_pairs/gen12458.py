from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("job_title").isin(["Manager", "Intern", "Engineer"]) & (F.col("salary") > 112000))
)

employees_stats = (
    filtered_employees
    .groupBy(
        F.when(F.col("job_title").isin(["Designer", "Manager"]), "priority").otherwise("other").alias("job_title_group"),
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
)

result = (
    employees_stats
    .filter(F.col("pct_manager") >= 59)
    .orderBy(F.col("analyst_count").desc())
)
