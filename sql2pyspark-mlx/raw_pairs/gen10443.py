from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & F.col("dept_id").isin([6, 40]))
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .orderBy("avg_salary")
)
