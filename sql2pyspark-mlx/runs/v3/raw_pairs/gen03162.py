from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([6, 23, 24, 35]) & F.col("full_name").isNotNull())
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
    .filter(F.col("l5_salary") > 45)
)
