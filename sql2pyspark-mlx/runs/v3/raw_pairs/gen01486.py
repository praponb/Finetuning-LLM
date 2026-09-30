from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Director", "Designer"]) & F.col("is_remote"))
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.min("salary").alias("min_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)
