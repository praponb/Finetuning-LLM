from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@company.co") & F.col("level").isin(["L1", "L4"]))
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.avg("salary").alias("avg_salary"))
)
