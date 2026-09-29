from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-08-01", "2025-08-15"))
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "job_title")
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
    )
)
