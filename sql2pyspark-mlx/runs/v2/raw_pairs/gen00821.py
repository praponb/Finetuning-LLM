from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "RETURNED", "IN_TRANSIT"]) & (F.col("carrier") == "FedEx"))
    .groupBy("status")
    .agg(F.max("delivered_date").alias("latest_delivered_date"))
)
