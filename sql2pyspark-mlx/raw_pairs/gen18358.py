from pyspark.sql import functions as F

result = (
    df_shipments
    .select("order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("carrier").isin(["Kerry", "DHL", "FedEx"]))
        .select("order_id")
    )
)
