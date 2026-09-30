from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("is_remote") & ~F.col("is_remote") & F.col("dept_id").isin([10, 18, 21]))
)

summary = (
    valid_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"), "level")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineer"),
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
    )
)

result = summary.select("hire_month", "level", "pct_engineer", "engineer_salary")
