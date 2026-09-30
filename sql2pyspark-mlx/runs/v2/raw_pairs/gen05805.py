from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["IN_TRANSIT", "RETURNED"]))
    .select(
        "order_id",
        "shipped_date",
        "carrier",
        F.length("status").alias("status_length"),
    )
)
