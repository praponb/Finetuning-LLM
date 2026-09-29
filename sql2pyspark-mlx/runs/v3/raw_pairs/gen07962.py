from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Kerry", "DHL"]))
    .select("status", "delivered_date")
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["LOST", "RETURNED", "DELIVERED"]))
        .select("status", "delivered_date")
    )
    .orderBy("delivered_date")
)
