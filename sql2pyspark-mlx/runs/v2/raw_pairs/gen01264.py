from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") != "Manager")
    .groupBy("is_remote")
    .agg(
        F.max("salary").alias("max_salary"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") > 82000)
    .select("is_remote", "max_salary", "avg_salary")
)
