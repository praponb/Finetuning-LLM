from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.max("salary").alias("max_salary"))
)
