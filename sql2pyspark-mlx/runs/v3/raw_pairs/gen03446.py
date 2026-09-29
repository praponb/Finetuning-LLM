from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
    .orderBy(F.col("hire_month").desc())
    .limit(5)
)
