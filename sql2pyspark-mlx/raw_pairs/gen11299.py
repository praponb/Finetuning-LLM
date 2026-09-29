from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 21) | (F.col("hire_date") >= "2025-01-01"))
    .groupBy("level", F.year("hire_date").alias("hire_year"))
    .agg(
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)
