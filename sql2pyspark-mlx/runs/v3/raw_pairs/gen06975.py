from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.avg("salary").alias("avg_salary"))
    .orderBy(F.col("hire_year").desc(), F.col("avg_salary").desc())
)
