from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L4")
    .agg(F.sum("salary").alias("total_salary"))
)
