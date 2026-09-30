from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", "level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)
