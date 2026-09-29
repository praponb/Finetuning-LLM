from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%ACK%"))
    .groupBy("is_remote", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("min_salary") > 92000)
    .orderBy(F.col("max_salary").desc())
)
