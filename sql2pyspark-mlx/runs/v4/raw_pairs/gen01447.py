from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") > "2023-06-06") | F.col("manager_id").isin([14, 24, 40]))
    .groupBy(
        "manager_id",
        F.when(F.col("salary") >= 120000, "high").when(F.col("salary") >= 64000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
)
