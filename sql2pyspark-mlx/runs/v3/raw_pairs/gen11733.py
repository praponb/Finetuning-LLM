from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"))
    .orderBy(F.col("dept_id").desc(), F.col("l5_count").desc())
    .limit(1)
)
