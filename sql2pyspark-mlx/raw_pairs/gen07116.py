from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 119000, "high").when(F.col("salary") >= 71000, "mid").otherwise("low").alias("salary_bucket"),
        F.year("hire_date").alias("hire_year"),
    )
    .agg(F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"))
    .filter(F.col("l2_count") >= 21)
)
