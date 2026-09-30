from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"), "level")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") > 614000)
)
