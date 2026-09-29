from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.count("*").alias("cnt"),
    )
)
