from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .filter(F.col("avg_salary") > 37)
    .orderBy("avg_salary")
)
