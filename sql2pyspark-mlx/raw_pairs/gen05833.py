from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary")).otherwise(0)).alias("intern_salary"),
    )
    .filter(F.col("intern_salary") < 64)
    .drop("intern_salary")
    .orderBy(F.col("unique_managers").desc())
)
