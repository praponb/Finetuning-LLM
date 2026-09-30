from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Designer", "Intern", "Analyst"]) & F.col("hire_date").between("2023-03-01", "2024-08-01"))
    .groupBy(
        F.when(F.col("salary") >= 134000, "high").when(F.col("salary") >= 53000, "mid").otherwise("low").alias("salary_bucket"),
        "is_remote",
        "manager_id",
    )
    .agg(F.sum("salary").alias("total_salary"))
    .orderBy("salary_bucket", F.col("total_salary").desc())
)
