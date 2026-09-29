from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") < 6)
    .select("hire_quarter", "total_count")
)
