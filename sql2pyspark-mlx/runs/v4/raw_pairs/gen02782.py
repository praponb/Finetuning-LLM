from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 161000, "high").when(F.col("salary") >= 68000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .filter(F.col("avg_salary") >= 62)
    .orderBy("salary_bucket")
    .limit(10)
)
