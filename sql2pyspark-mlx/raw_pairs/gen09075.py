from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "South")
    .select("opened_date")
    .union(
        df_shipments
        .filter(F.col("carrier").isin(["FedEx", "Flash", "UPS"]))
        .select("delivered_date")
    )
)
