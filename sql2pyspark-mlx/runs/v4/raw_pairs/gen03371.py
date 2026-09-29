from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (F.col("salary") > 102000))
    .cube(
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 70000, "mid").otherwise("low").alias("salary_bucket"),
        "job_title",
    )
    .agg(F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"))
)
