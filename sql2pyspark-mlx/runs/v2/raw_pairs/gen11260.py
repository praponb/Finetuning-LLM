from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "order_id",
        F.when(F.col("carrier").isin(["Flash", "UPS"]), "Tier 1").otherwise("Other").alias("carrier_group"),
    )
    .orderBy("carrier_group")
)
