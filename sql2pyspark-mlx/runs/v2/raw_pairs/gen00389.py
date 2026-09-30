from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["LOST", "DELIVERED", "IN_TRANSIT"]) | (F.col("cost") <= 30))
    .select(
        "id",
        "delivered_date",
        F.when(F.col("status").isin(["IN_TRANSIT", "RETURNED"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.concat_ws("/", "status", "carrier").alias("status_carrier_key"),
    )
)
