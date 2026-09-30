from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
        F.count("*").alias("cnt"),
    )
)
