from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
        F.max("salary").alias("max_salary"),
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
    .orderBy(F.col("l2_count").desc(), "hire_quarter")
)
