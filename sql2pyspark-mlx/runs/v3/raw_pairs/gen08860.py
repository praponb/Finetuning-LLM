from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)
