from pyspark.sql import functions as F

base = df_employees.filter(F.col("hire_date") < "2025-11-01")

summary = (
    base
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"))
)

result = summary.orderBy(F.col("l1_count").desc())
