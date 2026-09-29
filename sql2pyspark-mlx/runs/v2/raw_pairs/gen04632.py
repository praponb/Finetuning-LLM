from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([1, 3, 6]))
    .groupBy(
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 49000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary")).otherwise(0)).alias("intern_salary"),
    )
    .orderBy(F.col("avg_salary").desc(), "salary_bucket")
)
