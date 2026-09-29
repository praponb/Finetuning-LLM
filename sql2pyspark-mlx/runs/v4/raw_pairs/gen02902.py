from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
    .filter(F.col("l4_count") >= 31)
    .drop("l4_count")
)
