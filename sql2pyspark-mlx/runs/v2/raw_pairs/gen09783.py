from pyspark.sql import functions as F

result = (
    df_employees
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .orderBy("avg_salary")
)
