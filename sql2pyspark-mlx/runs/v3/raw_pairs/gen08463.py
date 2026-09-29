from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.month("hire_date").isin([5, 8, 10]) & ~F.col("job_title").isin(["Analyst", "Designer", "Manager"]) & F.col("full_name").like("H%"))
)

summary = (
    valid_employees
    .groupBy("is_remote", F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.max("salary").alias("max_salary"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
)

result = (
    summary
    .select("is_remote", "hire_quarter", "max_salary", "l4_count")
    .orderBy(F.col("l4_count").desc(), "is_remote", "hire_quarter")
    .limit(3)
)
