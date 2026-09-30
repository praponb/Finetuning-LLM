from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "carrier",
        "cost",
        "delivered_date",
        F.when(F.col("carrier").isin(["FedEx", "UPS"]), "Group A").otherwise("Other").alias("carrier_group"),
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
    )
)
