from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.count("*").alias("employee_count"))
    .orderBy(F.col("employee_count").desc(), "is_remote")
    .limit(5)
)
