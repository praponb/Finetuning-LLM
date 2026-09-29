from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.date_format("hire_date", "yyyy-MM").alias("hire_month"),
        F.when(F.col("salary") >= 156000, "high").when(F.col("salary") >= 52000, "mid").otherwise("low").alias("salary_bucket"),
        "dept_id",
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
        F.max("hire_date").alias("last_hire_date"),
    )
    .filter(F.col("director_salary") > 55)
    .orderBy("salary_bucket")
)
