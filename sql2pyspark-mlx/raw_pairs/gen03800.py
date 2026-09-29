from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
    )
    .orderBy("hire_month", "l2_salary")
    .limit(20)
)
