from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(F.sum("salary").alias("total_salary"))
    .orderBy("level")
)
