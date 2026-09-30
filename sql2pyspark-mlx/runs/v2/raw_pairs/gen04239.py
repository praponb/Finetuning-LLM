from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") > 100000)
)
