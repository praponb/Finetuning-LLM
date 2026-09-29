from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 95000)
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"), "is_remote")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("avg_salary") >= 94000)
)
