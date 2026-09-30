from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "job_title")
    .pivot("level", ["L2", "L1"])
    .agg(F.avg("salary").alias("value_sum"))
)
