from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 58000)
    .groupBy("is_remote", F.month("hire_date").alias("hire_month"))
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") > 74000)
)
