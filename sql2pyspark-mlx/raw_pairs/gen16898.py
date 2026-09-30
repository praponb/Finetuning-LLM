from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 148000, "high").when(F.col("salary") >= 73000, "mid").otherwise("low").alias("salary_bucket"),
        "is_remote",
    )
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("l5_salary") > 33)
)
