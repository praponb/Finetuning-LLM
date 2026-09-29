from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
    )
    .filter(F.col("avg_salary") < 23)
)
