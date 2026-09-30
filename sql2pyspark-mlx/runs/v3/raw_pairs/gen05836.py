from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum("salary").alias("total_salary"),
        F.min("salary").alias("min_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .orderBy("hire_month")
    .limit(5)
)
