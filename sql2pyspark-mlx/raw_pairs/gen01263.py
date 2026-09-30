from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 80000) & F.col("email").like("%@gmail.com"))
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(F.min("hire_date").alias("min_hire_date"))
)
