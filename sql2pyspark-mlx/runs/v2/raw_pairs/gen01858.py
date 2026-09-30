from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 58000, "mid").otherwise("low").alias("salary_bucket"),
        "level",
    )
    .agg(
        F.min("salary").alias("min_salary"),
        F.max("hire_date").alias("latest_hire_date"),
    )
    .filter(F.col("min_salary") >= 114000)
    .orderBy("salary_bucket")
)
