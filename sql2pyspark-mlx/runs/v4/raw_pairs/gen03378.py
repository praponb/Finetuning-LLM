from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "DHL", "FedEx"]))
    .groupBy("status")
    .agg(F.max("shipped_date").alias("latest_shipped_date"))
    .orderBy(F.col("latest_shipped_date").desc())
)
