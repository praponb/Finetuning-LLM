from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select(
        "id",
        "status",
        "order_id",
        "delivered_date",
        F.when(F.col("carrier") == "FedEx", 1).otherwise(0).alias("is_fedex"),
    )
)
