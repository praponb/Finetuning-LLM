from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(F.max("salary").alias("max_salary"))
    .orderBy("max_salary", F.col("hire_month").desc())
)
