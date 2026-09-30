from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 153000, "high").when(F.col("salary") >= 64000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("hire_date").alias("last_hire_date"),
    )
    .filter(F.col("row_count") >= 4)
    .orderBy(F.col("last_hire_date").desc(), F.col("row_count").desc())
)
