from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy(
        F.year("hire_date").alias("hire_year"),
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("hire_date").alias("last_hire_date"),
    )
    .filter(F.col("avg_salary") < 43)
    .orderBy(F.col("last_hire_date").desc())
)
