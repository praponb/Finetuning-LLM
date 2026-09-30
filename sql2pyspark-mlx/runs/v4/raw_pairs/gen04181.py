from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Dav%") & ((F.col("level") == "L3") | (F.col("hire_date") < "2025-06-29")))
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
    )
    .orderBy("min_salary")
)
