from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-03-06", "2025-04-01") & (F.col("order_id") == 10))
    .select(
        "id",
        "order_id",
        "shipped_date",
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
    )
)
