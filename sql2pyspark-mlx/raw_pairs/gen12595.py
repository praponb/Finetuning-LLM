from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.col("job_title").isin(["Analyst", "Intern", "Engineer"]) & ~F.col("is_remote") & F.col("full_name").isNotNull())
)

summary = (
    recent_employees
    .groupBy("job_title", "is_remote")
    .agg(F.countDistinct("salary").alias("unique_levels"))
)

result = (
    summary
    .filter(F.col("unique_levels") < 9)
    .orderBy("job_title")
)
