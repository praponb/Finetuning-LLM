from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([6, 11, 32]))
    .select("id", "level", F.date_trunc("year", "hire_date").alias("hire_date_period"))
    .orderBy(F.col("id").desc())
)
