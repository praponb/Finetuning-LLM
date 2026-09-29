from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(60000, 105000))
    .groupBy("dept_id", "is_remote")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .filter(F.col("avg_salary") >= 73)
)
