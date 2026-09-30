from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy(
        F.when(F.col("salary") >= 132000, "high").when(F.col("salary") >= 74000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") > 799000)
)
