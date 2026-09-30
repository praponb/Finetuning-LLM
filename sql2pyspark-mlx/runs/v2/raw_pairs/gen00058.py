from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([9, 18, 21]))
    .groupBy("job_title", "is_remote")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.min("hire_date").alias("first_hire_date"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)
