from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Manager") & (F.col("manager_id") == 6))
    .groupBy("level", "is_remote")
    .agg(F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"))
)
