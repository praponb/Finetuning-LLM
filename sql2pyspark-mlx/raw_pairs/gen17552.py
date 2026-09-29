from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"))
)
