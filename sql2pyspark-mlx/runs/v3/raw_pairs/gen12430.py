from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.min("salary").alias("min_salary"))
    .filter(F.col("min_salary") >= 110000)
    .select("hire_month", "min_salary")
)
