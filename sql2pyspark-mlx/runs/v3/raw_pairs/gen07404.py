from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .groupBy("carrier")
    .agg(F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"))
)
