from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "DHL", "UPS"]))
    .select("carrier", "cost", F.lower("carrier").alias("carrier_lower"))
)
