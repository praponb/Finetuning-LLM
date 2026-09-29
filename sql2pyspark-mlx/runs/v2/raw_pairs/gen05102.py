from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(~F.col("carrier").isin(["Kerry", "FedEx", "Flash"]))
    .withColumn("is_dhl", F.when(F.col("carrier") == "DHL", 1).otherwise(0))
)

shipments_stats = (
    valid_shipments
    .groupBy("order_id")
    .agg(F.count("*").alias("num_shipments"))
)

result = shipments_stats.orderBy(F.col("order_id").desc(), F.col("num_shipments").desc())
