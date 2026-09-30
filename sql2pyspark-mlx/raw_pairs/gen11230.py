from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "DHL", "FedEx"]))
    .select("shipped_date", "cost", "delivered_date")
    .orderBy("shipped_date")
)
