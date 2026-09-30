from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "order_id",
        "status",
        "carrier",
        F.when(F.col("carrier").isin(["Kerry", "DHL"]), "Tier 1").otherwise("Other").alias("carrier_group"),
        F.trim("carrier").alias("clean_carrier"),
    )
)
