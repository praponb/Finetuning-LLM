from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 159000, "high").when(F.col("salary") >= 51000, "mid").otherwise("low").alias("salary_bucket"),
        "job_title",
    )
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.countDistinct("salary").alias("unique_depts"),
    )
    .filter(F.col("unique_depts") < 5)
    .select("salary_bucket", "job_title", "first_hire_date", "unique_depts")
)
