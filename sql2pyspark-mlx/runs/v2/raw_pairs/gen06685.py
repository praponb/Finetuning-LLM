from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2025-02-01")
    .groupBy("job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
    .orderBy(F.col("total_salary").desc(), "pct_l1")
)
