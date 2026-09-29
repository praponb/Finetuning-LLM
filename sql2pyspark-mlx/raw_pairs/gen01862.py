from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
    )
    .orderBy(F.col("min_salary").desc())
)
