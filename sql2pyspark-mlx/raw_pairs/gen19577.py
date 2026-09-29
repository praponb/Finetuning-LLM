from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("level").isin(["L4", "L2", "L5"]) | (F.col("salary") < 116000))
)

salary_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("salary") >= 120000, "high").when(F.col("salary") >= 54000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
    )
)

result = salary_bucket_summary.orderBy(F.col("designer_count").desc())
