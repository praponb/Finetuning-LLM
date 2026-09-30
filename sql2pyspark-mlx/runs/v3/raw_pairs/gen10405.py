from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .select(
        "carrier",
        F.when(F.col("carrier").isin(["FedEx", "Kerry"]), "Core").otherwise("Other").alias("carrier_group"),
    )
    .orderBy("carrier", F.col("carrier_group").desc())
)
