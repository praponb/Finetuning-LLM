from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") | F.col("manager_id").isin([3, 8, 26]))
    .groupBy("level", "job_title")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.min("hire_date").alias("min_hire_date"),
    )
    .orderBy(F.col("stddev_salary").desc())
)
