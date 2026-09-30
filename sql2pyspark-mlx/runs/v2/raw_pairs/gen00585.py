from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 148000, "high").when(F.col("salary") >= 42000, "mid").otherwise("low").alias("salary_bucket"),
        F.month("hire_date").alias("hire_month"),
    )
    .agg(F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"))
    .filter(F.col("l4_count") >= 29)
)
