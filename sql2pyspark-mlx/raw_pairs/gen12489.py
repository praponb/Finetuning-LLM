from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(112000, 115000))
    .select("salary", "job_title")
)
