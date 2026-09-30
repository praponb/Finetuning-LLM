from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@GMAIL.COM"))
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .orderBy(F.col("avg_salary").desc())
)
