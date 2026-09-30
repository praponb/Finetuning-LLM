from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Intern"]))
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.countDistinct("salary").alias("unique_job_titles"),
    )
    .filter(F.col("unique_job_titles") > 5)
    .select("hire_month", "min_hire_date", "unique_job_titles")
)
