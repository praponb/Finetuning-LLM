from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.count("*").alias("shipment_count"),
    )
    .filter(F.col("shipment_count") > 4)
    .drop("shipment_count")
)
