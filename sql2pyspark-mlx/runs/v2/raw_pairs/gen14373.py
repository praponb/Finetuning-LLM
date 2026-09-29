from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("carrier").isNotNull())

carrier_summary = (
    recent_shipments
    .groupBy("carrier")
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("*").alias("shipment_count"),
    )
)

result = (
    carrier_summary
    .filter(F.col("shipment_count") > 5)
    .select("carrier", "min_cost", "shipment_count")
)
