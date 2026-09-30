from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.length("email") >= 14)

salary_bucket_summary = (
    filtered_employees
    .groupBy(
        F.when(F.col("salary") >= 157000, "high").when(F.col("salary") >= 49000, "mid").otherwise("low").alias("salary_bucket"),
        "job_title",
    )
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    salary_bucket_summary
    .filter(F.col("avg_salary") > 131000)
    .select("salary_bucket", "avg_salary")
    .orderBy("avg_salary", "salary_bucket")
)
