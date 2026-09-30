from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.min("salary").alias("min_salary"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
    )
    .filter(F.col("min_salary") < 121000)
    .select("hire_quarter", "min_salary", "l2_count")
)
