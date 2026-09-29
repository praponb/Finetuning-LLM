from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() & F.col("is_remote"))
    .groupBy("job_title")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .orderBy(F.col("stddev_salary").desc(), "job_title")
    .limit(50)
)
