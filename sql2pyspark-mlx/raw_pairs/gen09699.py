from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "delivered_date",
        "order_id",
        "carrier",
        F.trim("status").alias("clean_status"),
    )
)
