from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L5", "L3", "L1"]) & F.col("manager_id").isin([18, 24]))
    .agg(F.max("salary").alias("max_salary"))
)
