from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "status",
        "delivered_date",
        "shipped_date",
        F.upper("status").alias("status_upper"),
    )
)
