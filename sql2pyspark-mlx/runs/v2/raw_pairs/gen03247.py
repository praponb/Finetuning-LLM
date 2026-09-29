from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)
