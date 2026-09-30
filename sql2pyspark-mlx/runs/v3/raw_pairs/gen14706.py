from pyspark.sql import functions as F

src = df_employees.filter(F.col("level").isin(["L2", "L5"]))

agg = (
    src
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 124000, "high").when(F.col("salary") >= 56000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.avg("salary").alias("avg_salary"))
)

result = agg.select("job_title", "salary_bucket", "avg_salary")
