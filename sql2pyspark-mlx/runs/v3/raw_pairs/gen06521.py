from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
        "is_remote",
        F.when(F.col("salary") >= 135000, "high").when(F.col("salary") >= 76000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.avg("salary").alias("avg_salary"))
)
