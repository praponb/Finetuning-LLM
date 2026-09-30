from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.col("delivered_date") > "2025-01-01")
    .withColumn("cost_whole", F.ceil("cost"))
)

carrier_summary = (
    valid_shipments
    .groupBy(
        "carrier",
        F.when(F.col("carrier").isin(["Kerry", "UPS"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
        F.count("*").alias("shipment_count"),
    )
)

result = carrier_summary.orderBy("flash_count", "carrier")
