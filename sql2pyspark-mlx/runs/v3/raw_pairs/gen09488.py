from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "Kerry"]))
    .agg(F.count("*").alias("shipment_count"))
    .orderBy(F.col("shipment_count").desc())
)
