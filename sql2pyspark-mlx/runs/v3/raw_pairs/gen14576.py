from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "UPS", "Flash"]))
    .groupBy("carrier", "status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
    )
    .filter(F.col("cost_count") < 8)
    .orderBy("cost_count", F.col("kerry_count").desc())
)
