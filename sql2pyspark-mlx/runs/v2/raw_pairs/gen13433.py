from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (F.year("hire_date") == 2023) & F.col("is_remote"))
    .groupBy("job_title")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .orderBy(F.col("stddev_salary").desc())
)
