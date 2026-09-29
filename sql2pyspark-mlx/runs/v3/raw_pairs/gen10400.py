from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(F.count("*").alias("num_employees"))
    .orderBy(F.col("num_employees").desc(), "hire_quarter")
    .limit(50)
)
