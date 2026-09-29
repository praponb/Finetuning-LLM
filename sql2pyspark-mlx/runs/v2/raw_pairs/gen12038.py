from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
        "is_remote",
        "job_title",
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
    )
    .orderBy(F.col("designer_salary").desc())
)
