from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "is_remote")
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") < 107000)
)
