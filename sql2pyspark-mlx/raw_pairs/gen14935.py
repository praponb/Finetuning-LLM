from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date").between("2024-01-08", "2025-01-01") & F.col("order_id").isNotNull()) | (F.year("delivered_date") >= 2025))
    .select(
        "id",
        "order_id",
        "delivered_date",
        F.concat("status", F.lit("/"), "carrier").alias("status_carrier"),
    )
)
