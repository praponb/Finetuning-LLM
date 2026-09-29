from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"))
    .orderBy(F.col("l3_count").desc())
)
