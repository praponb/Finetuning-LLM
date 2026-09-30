from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 126000, "high").when(F.col("salary") >= 55000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.max("salary").alias("max_salary"),
    )
)
