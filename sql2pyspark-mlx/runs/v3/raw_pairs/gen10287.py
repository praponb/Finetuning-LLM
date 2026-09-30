from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("order_id").isNull() & (F.col("cost") >= 36)))
    .select(
        "id",
        "delivered_date",
        "order_id",
        "status",
        F.length("carrier").alias("carrier_length"),
    )
)
