from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-05-01", "2024-05-01") & F.col("full_name").isNotNull() & F.col("level").isin(["L4", "L1"]))
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .orderBy("dept_id", F.col("avg_salary").desc())
)
