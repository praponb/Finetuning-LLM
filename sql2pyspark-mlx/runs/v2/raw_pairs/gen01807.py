from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNull())
    .groupBy("job_title")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") < 472000)
)
