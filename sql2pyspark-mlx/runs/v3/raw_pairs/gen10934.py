from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 22) & (F.col("job_title") != "Analyst"))
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.avg("salary").alias("avg_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .orderBy("avg_salary")
)
