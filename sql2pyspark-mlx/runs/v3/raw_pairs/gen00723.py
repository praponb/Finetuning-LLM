from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(F.max("hire_date").alias("last_hire_date"))
    .orderBy("hire_month", "last_hire_date")
)
