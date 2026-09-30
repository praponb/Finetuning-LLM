from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") < 84000)
    .drop("max_salary")
    .orderBy("job_title")
)
