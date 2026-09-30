from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 128000, "high").when(F.col("salary") >= 48000, "mid").otherwise("low").alias("salary_bucket"),
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
    )
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.min("salary").alias("min_salary"),
    )
)
