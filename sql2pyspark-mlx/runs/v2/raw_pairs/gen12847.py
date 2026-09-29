from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-12-28", "2025-07-26") & F.col("manager_id").isin([2, 16, 20, 35]))
    .groupBy("job_title")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("pct_manager") > 61)
)
