from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "FedEx", "DHL"]) & (F.col("delivered_date") > "2024-10-01"))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") < 45)
    .orderBy(F.col("avg_cost").desc(), "status")
    .limit(10)
)
