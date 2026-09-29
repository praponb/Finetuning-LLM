from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.year("hire_date") == 2024)

summary = (
    valid_employees
    .groupBy(
        F.when(F.col("salary") >= 128000, "high").when(F.col("salary") >= 72000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
)

result = (
    summary
    .select("salary_bucket", "cnt")
    .orderBy(F.col("cnt").desc(), "salary_bucket")
)
