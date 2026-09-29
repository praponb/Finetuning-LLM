from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") < 42)
    .select("hire_month", "stddev_salary", "avg_salary")
)
