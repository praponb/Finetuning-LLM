from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "Chiang Mai"]))
    .groupBy("region")
    .agg(F.count("*").alias("row_count"))
    .orderBy("region")
)
