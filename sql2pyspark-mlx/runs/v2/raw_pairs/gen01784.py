from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.year("hire_date") == 2025)
    .groupBy(
        F.when(F.col("salary") >= 156000, "high").when(F.col("salary") >= 68000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"))
    .filter(F.col("designer_count") > 4)
    .select("salary_bucket", "designer_count")
    .orderBy(F.col("designer_count").desc())
)
