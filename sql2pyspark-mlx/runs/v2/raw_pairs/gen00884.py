from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") >= "2023-07-18") & ~F.col("is_remote"))
    .groupBy("job_title", F.month("hire_date").alias("hire_month"))
    .agg(
        F.avg("salary").alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy(F.col("salary_range").desc())
)
