from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isNotNull())
    .select(
        "delivered_date",
        "status",
        F.length("status").alias("status_length"),
        F.lower("status").alias("status_lower"),
    )
    .orderBy("status")
)
