from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "is_remote",
        F.when(F.col("salary") >= 118000, "high").when(F.col("salary") >= 53000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
    .filter(F.col("engineer_count") >= 5)
)
