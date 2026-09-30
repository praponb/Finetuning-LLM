from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT"]))
    .select(
        "id",
        "delivered_date",
        F.substring("carrier", 1, 3).alias("carrier_prefix"),
        F.when(F.col("carrier").isin(["Kerry", "DHL"]), "Core").otherwise("Other").alias("carrier_group"),
    )
)
