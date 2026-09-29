from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(F.avg("salary").alias("avg_salary"))
)
