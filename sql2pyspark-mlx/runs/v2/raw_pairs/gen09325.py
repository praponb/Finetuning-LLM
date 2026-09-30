from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "FedEx"]))
    .select("status")
    .distinct()
)
