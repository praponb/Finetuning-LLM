from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") >= 13) & (F.col("hire_date") >= "2025-09-01"))
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"), "job_title")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
)
