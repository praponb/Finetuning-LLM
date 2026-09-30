from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") <= "2025-05-01") & F.col("manager_id").isin([18, 19, 24]))
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy(F.col("dept_id").desc())
)
