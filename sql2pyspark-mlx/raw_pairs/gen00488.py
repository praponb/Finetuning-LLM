from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("dept_id").isin([14, 18]))
    .groupBy(
        F.when(F.col("salary") >= 121000, "high").when(F.col("salary") >= 58000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)
