from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Analyst", "Director"]) & F.col("manager_id").isin([37, 39]) & (F.upper(F.trim("email")) == "IVY@GMAIL.COM"))
    .groupBy(F.month("hire_date").alias("hire_month"), "job_title")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.sum("salary").alias("total_salary"),
        F.max("hire_date").alias("last_hire_date"),
    )
)
