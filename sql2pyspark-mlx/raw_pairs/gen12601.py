from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Kerry") & F.col("shipped_date").between("2025-05-01", "2025-08-01"))
    .select(
        "id",
        "carrier",
        "order_id",
        F.concat_ws("-", "status", "carrier").alias("status_carrier_key"),
    )
)
