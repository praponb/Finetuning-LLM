from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 117000)
    .groupBy("job_title")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
    )
    .filter(F.col("manager_salary") < 36)
)
