from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "job_title")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.max("hire_date").alias("latest_hire_date"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") < 138000)
    .drop("max_salary")
    .orderBy("level")
)
