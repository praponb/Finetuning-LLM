from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "level",
        F.when(F.col("salary") >= 144000, "high").when(F.col("salary") >= 78000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") > 84000)
    .orderBy(F.col("avg_salary").desc(), "level", "salary_bucket")
    .limit(5)
)
