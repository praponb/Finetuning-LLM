from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"))
    .orderBy("ups_count")
)
