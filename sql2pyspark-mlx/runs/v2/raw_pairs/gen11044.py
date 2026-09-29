from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([8, 15, 30]) & F.col("full_name").isNull() & F.col("manager_id").isNotNull())
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(F.min("hire_date").alias("first_hire_date"))
)
