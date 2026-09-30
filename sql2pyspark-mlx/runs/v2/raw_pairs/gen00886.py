from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "Chiang Mai", "Berlin"]))
    .groupBy("city")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc())
)
