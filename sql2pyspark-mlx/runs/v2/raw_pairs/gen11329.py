from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
    )
)
