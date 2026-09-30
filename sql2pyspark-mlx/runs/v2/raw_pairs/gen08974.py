from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") != "LOST") & F.col("carrier").isin(["FedEx", "Flash"]) & (F.col("order_id") == 8))
    .select("carrier", "shipped_date", "delivered_date")
)
