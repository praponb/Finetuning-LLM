from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") >= 73000)
    .select("hire_month", "first_hire_date", "avg_salary")
)
