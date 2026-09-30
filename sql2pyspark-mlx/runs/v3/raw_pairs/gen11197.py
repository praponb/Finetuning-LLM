from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        "status",
        F.when(F.col("carrier").isin(["DHL", "UPS"]), "priority").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.count("*").alias("num_shipments"),
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.max("delivered_date").alias("latest_delivered_date"),
    )
    .orderBy("carrier_group")
)
