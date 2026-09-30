from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"))
    .orderBy("status", "carrier")
    .limit(3)
)
