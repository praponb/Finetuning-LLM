from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
    )
)
