from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L1", "L2", "L3"]))
    .groupBy("level")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)
