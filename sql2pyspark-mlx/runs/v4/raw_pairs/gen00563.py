from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 7)
    .select("status", "row_count")
)
