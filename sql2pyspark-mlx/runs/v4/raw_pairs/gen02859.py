from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(F.sum("salary").alias("total_salary"))
    .filter(F.col("total_salary") < 663000)
    .orderBy(F.col("total_salary").desc())
)
