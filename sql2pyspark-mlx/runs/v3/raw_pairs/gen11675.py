from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
    )
)
