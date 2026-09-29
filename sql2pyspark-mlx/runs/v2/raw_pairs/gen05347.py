from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & F.col("carrier").isin(["Kerry", "FedEx"]))
    .select("id", "delivered_date", "carrier", "status")
)
