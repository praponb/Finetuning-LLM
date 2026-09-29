from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "job_title")
    .pivot("level", ["L1", "L3"])
    .agg(F.sum("salary").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
