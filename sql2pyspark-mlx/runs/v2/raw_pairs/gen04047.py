from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2023-04-01")
    .select(
        "id",
        "delivered_date",
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_date_label"),
    )
)
