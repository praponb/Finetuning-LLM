from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"), "manager_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
    )
    .filter(F.col("designer_salary") > 67)
    .orderBy(F.col("designer_salary").desc(), "hire_month", "manager_id")
    .limit(5)
)
