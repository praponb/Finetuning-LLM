from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.month("hire_date").alias("hire_month"),
        F.year("hire_date").alias("hire_year"),
        "job_title",
    )
    .agg(F.max("salary").alias("max_salary"))
    .filter(F.col("max_salary") >= 139000)
    .orderBy("hire_year", "job_title", "hire_month")
    .limit(1)
)
