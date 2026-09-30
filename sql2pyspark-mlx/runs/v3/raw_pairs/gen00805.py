from pyspark.sql import functions as F

result = (
    df_employees
    .rollup(
        "is_remote",
        F.when(F.col("salary") >= 145000, "high").when(F.col("salary") >= 70000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.max("hire_date").alias("latest_hire_date"))
)
